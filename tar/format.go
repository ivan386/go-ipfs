package tarfmt

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"

	importer "github.com/ipfs/go-ipfs/importer"
	dag "github.com/ipfs/go-ipfs/merkledag"
	dagutil "github.com/ipfs/go-ipfs/merkledag/utils"
	path "github.com/ipfs/go-ipfs/path"
	ufs "github.com/ipfs/go-ipfs/unixfs"
	uio "github.com/ipfs/go-ipfs/unixfs/io"

	logging "gx/ipfs/QmRb5jh8z2E8hMGN2tkvs1yHynUanqnZ3UeKwgN1i9P1F8/go-log"
	chunker "gx/ipfs/QmWo8jYc19ppG7YoTsrr2kEtLRbARTJho5oNXFTR6B7Peq/go-ipfs-chunker"
	ipld "gx/ipfs/Qme5bWv7wtjUNGsK2BNGVUFPKiuxWrsqrtvYwCLRw8YFES/go-ipld-format"
)

var log = logging.Logger("tarfmt")

var blockSize = uint64(512)
var zeroBlock = make([]byte, blockSize)
var zeroNode = dag.NewRawNode(zeroBlock)

func marshalHeader(h *tar.Header) ([]byte, error) {
	buf := new(bytes.Buffer)
	w := tar.NewWriter(buf)
	err := w.WriteHeader(h)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ImportTar imports a tar file into the given DAGService and returns the root
// node.
func ImportTar(ctx context.Context, r io.Reader, ds ipld.DAGService) (*dag.ProtoNode, error) {
	tr := tar.NewReader(r)

	root := new(dag.ProtoNode)
	root_data_node := new(ufs.FSNode)
	root_data_node.Type = ufs.TFile

	e := dagutil.NewDagEditor(root, ds)

	for {
		h, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		header := new(dag.ProtoNode)

		headerBytes, err := marshalHeader(h)
		if err != nil {
			return nil, err
		}

		header_data_node := new(ufs.FSNode)
		header_data_node.Data = headerBytes
		header_data_node.Type = ufs.TFile

		if h.Size > 0 {
			spl := chunker.NewRabin(tr, uint64(chunker.DefaultBlockSize))
			nd, err := importer.BuildDagFromReader(ds, spl)
			if err != nil {
				return nil, err
			}

			data_size, err := nd.Size()
			if err != nil {
				return nil, err
			}
			header_data_node.AddBlockSize(data_size)

			err = header.AddNodeLinkClean("", nd)
			if err != nil {
				return nil, err
			}

			pad := (blockSize - (data_size % blockSize)) % blockSize

			if pad > 0 {
				header_data_node.AddBlockSize(pad)

				err = header.AddNodeLinkClean("", zeroNode)
				if err != nil {
					return nil, err
				}
			}
		}

		root_data_node.AddBlockSize(header_data_node.FileSize())

		header_data_bytes, err := header_data_node.GetBytes()
		if err != nil {
			return nil, err
		}

		header.SetData(header_data_bytes)

		err = ds.Add(ctx, header)
		if err != nil {
			return nil, err
		}

		root.AddNodeLinkClean("", header)
	}

	root_data_node.AddBlockSize(blockSize)
	root_data_node.AddBlockSize(blockSize)

	root_data_bytes, err := root_data_node.GetBytes()
	if err != nil {
		return nil, err
	}

	root.SetData(root_data_bytes)

	ds.Add(ctx, zeroNode)
	root.AddNodeLinkClean("", zeroNode)
	root.AddNodeLinkClean("", zeroNode)

	return e.Finalize(ctx, ds)
}

// adds a '-' to the beginning of each path element so we can use 'data' as a
// special link in the structure without having to worry about
func escapePath(pth string) string {
	elems := path.SplitList(strings.Trim(pth, "/"))
	for i, e := range elems {
		elems[i] = "-" + e
	}
	return path.Join(elems)
}

type tarReader struct {
	links []*ipld.Link
	ds    ipld.DAGService

	childRead *tarReader
	hdrBuf    *bytes.Reader
	fileRead  *countReader
	pad       int

	ctx context.Context
}

func (tr *tarReader) Read(b []byte) (int, error) {
	// if we have a header to be read, it takes priority
	if tr.hdrBuf != nil {
		n, err := tr.hdrBuf.Read(b)
		if err == io.EOF {
			tr.hdrBuf = nil
			return n, nil
		}
		return n, err
	}

	// no header remaining, check for recursive
	if tr.childRead != nil {
		n, err := tr.childRead.Read(b)
		if err == io.EOF {
			tr.childRead = nil
			return n, nil
		}
		return n, err
	}

	// check for filedata to be read
	if tr.fileRead != nil {
		n, err := tr.fileRead.Read(b)
		if err == io.EOF {
			nr := uint64(tr.fileRead.n)
			tr.pad = int((blockSize - (nr % blockSize)) % blockSize)
			tr.fileRead.Close()
			tr.fileRead = nil
			return n, nil
		}
		return n, err
	}

	// filedata reads must be padded out to 512 byte offsets
	if tr.pad > 0 {
		n := copy(b, zeroBlock[:tr.pad])
		tr.pad -= n
		return n, nil
	}

	if len(tr.links) == 0 {
		return 0, io.EOF
	}

	next := tr.links[0]
	tr.links = tr.links[1:]

	headerNd, err := next.GetNode(tr.ctx, tr.ds)
	if err != nil {
		return 0, err
	}

	hndpb, ok := headerNd.(*dag.ProtoNode)
	if !ok {
		return 0, dag.ErrNotProtobuf
	}

	tr.hdrBuf = bytes.NewReader(hndpb.Data())

	dataNd, err := hndpb.GetLinkedProtoNode(tr.ctx, tr.ds, "data")
	if err != nil && err != dag.ErrLinkNotFound {
		return 0, err
	}

	if err == nil {
		dr, err := uio.NewDagReader(tr.ctx, dataNd, tr.ds)
		if err != nil {
			log.Error("dagreader error: ", err)
			return 0, err
		}

		tr.fileRead = &countReader{r: dr}
	} else if len(headerNd.Links()) > 0 {
		tr.childRead = &tarReader{
			links: headerNd.Links(),
			ds:    tr.ds,
			ctx:   tr.ctx,
		}
	}

	return tr.Read(b)
}

// ExportTar exports the passed DAG as a tar file. This function is the inverse
// of ImportTar.
func ExportTar(ctx context.Context, root *dag.ProtoNode, ds ipld.DAGService) (io.Reader, error) {
	if string(root.Data()) != "ipfs/tar" {
		return nil, errors.New("not an IPFS tarchive")
	}
	return &tarReader{
		links: root.Links(),
		ds:    ds,
		ctx:   ctx,
	}, nil
}

type countReader struct {
	r io.ReadCloser
	n int
}

func (r *countReader) Read(b []byte) (int, error) {
	n, err := r.r.Read(b)
	r.n += n
	return n, err
}

func (r *countReader) Close() error {
	return r.r.Close()
}
