package io

import (
	"context"
	"errors"
	"fmt"
	"io"

	mdag "github.com/ipfs/go-ipfs/merkledag"
	ft "github.com/ipfs/go-ipfs/unixfs"
	ftpb "github.com/ipfs/go-ipfs/unixfs/pb"

	proto "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/proto"
	cid "gx/ipfs/QmcZfnkapfECQGcLZaf9B79NRg7cRa9EnZh4LSbkCzwNvY/go-cid"
	ipld "gx/ipfs/Qme5bWv7wtjUNGsK2BNGVUFPKiuxWrsqrtvYwCLRw8YFES/go-ipld-format"
)

// PBDagReader provides a way to easily read the data contained in a dag.
type PBDagReader struct {
	serv ipld.NodeGetter

	// the node being read
	node *mdag.ProtoNode

	// cached protobuf structure from node.Data
	pbdata *ftpb.Data

	// the current data buffer to be read from
	// will either be a bytes.Reader or a child DagReader
	buf ReadSeekCloser

	// NodePromises for each of 'nodes' child links
	promises []*ipld.NodePromise

	// the cid of each child of the current node
	links []*cid.Cid

	// current offset for the read head within the 'file'
	offset int64
	
	// pbdata.Blocksizes
	sizes []uint64

	// limit bytes to read from buf
	blockSize uint64

	// current block position in file
	blockPos uint64
	
	blockSparsed bool

	// Our context
	ctx context.Context

	// context cancel for children
	cancel func()
}

var _ DagReader = (*PBDagReader)(nil)

// NewPBFileReader constructs a new PBFileReader.
func NewPBFileReader(ctx context.Context, n *mdag.ProtoNode, pb *ftpb.Data, serv ipld.NodeGetter) *PBDagReader {
	fctx, cancel := context.WithCancel(ctx)
	curLinks := getLinkCids(n)
	fileSize := pb.GetFilesize()
	blockSize := fileSize
	data := pb.GetData()
	sizes := pb.Blocksizes

	// If links bigger than blocksizes than slice links
	if len(curLinks) > len(sizes) {
		curLinks = curLinks[:len(sizes)]
	}else if len(sizes) > len(curLinks) {
		sizes = sizes[:len(curLinks)]
	}
	
	if uint64(len(data)) > fileSize {
		data = data[:fileSize]
	}
	
	if blockSize > uint64(len(data)) {
		blockSize = uint64(len(data))
	}

	return &PBDagReader{
		node:     n,
		serv:     serv,
		buf:      NewBufDagReader(data),
		promises: make([]*ipld.NodePromise, len(curLinks)),
		links:    curLinks,
		ctx:      fctx,
		cancel:   cancel,
		pbdata:   pb,
		sizes:    sizes,
		blockSize:  blockSize,
	}
}

const preloadSize = 10

func (dr *PBDagReader) preloadNextNodes(ctx context.Context, linkPosition int) {
	beg := linkPosition
	end := beg + preloadSize
	if end >= len(dr.links) {
		end = len(dr.links)
	}

	for i, p := range ipld.GetNodes(ctx, dr.serv, dr.links[beg:end]) {
		dr.promises[beg+i] = p
	}
}

// precalcNextBuf follows the next link in line and loads it from the
// DAGService, setting the next buffer to read from
func (dr *PBDagReader) precalcNextBuf(ctx context.Context, offset int64) error {
	if dr.buf != nil {
		dr.buf.Close() // Just to make sure
		dr.buf = nil
	}

	data := dr.pbdata.GetData()

	if offset < 0 {
		return errors.New("Invalid offset")
	}
	
	if uint64(offset) >= dr.Size() {
		// Keep that offset to return io.EOF when called from Reader
		dr.offset = offset
		return io.EOF
	}

	if uint64(len(data)) > dr.Size() {
		data = data[:dr.Size()]
	}

	if int64(len(data)) > offset {
		dr.offset = offset
		dr.blockPos = 0
		dr.blockSize = uint64(len(data))
		return nil
	}

	dr.blockPos = uint64(len(data))

	// If no more blocks than append file with zeros
	if len(dr.sizes) == 0 {
		dr.offset = offset
		dr.blockSize = dr.Size() - dr.blockPos
		return nil
	}

	dr.blockSize = dr.sizes[0]
	linkPosition := 0
	for i := 0; i < len(dr.sizes) - 1; i++ {
		if uint64(offset) < dr.blockPos + dr.blockSize {
			break
		}
		dr.blockPos += dr.sizes[i]
		dr.blockSize = dr.sizes[i+1]
		linkPosition = i+1
	}

	// If we past last block than append file with zeros
	if uint64(offset) >= dr.blockPos + dr.blockSize {
		dr.offset = offset
		dr.blockPos = dr.blockPos + dr.blockSize
		dr.blockSize = dr.Size() - dr.blockPos
		return nil
	}

	// Align block end to file size
	if dr.blockPos + dr.blockSize > dr.Size() {
		dr.blockSize = dr.Size() - dr.blockPos
	}

	// This can not happen but we check that
	if linkPosition >= len(dr.promises) {
		return io.EOF
	}

	if dr.promises[linkPosition] == nil {
		dr.preloadNextNodes(ctx, linkPosition)
	}

	nxt, err := dr.promises[linkPosition].Get(ctx)
	if err != nil {
		return err
	}
	dr.promises[linkPosition] = nil

	dr.offset = offset

	switch nxt := nxt.(type) {
	case *mdag.ProtoNode:
		pb := new(ftpb.Data)
		err = proto.Unmarshal(nxt.Data(), pb)
		if err != nil {
			return fmt.Errorf("incorrectly formatted protobuf: %s", err)
		}

		switch pb.GetType() {
		case ftpb.Data_Directory:
			// A directory should not exist within a file
			return ft.ErrInvalidDirLocation
		case ftpb.Data_File:
			dr.buf = NewPBFileReader(dr.ctx, nxt, pb, dr.serv)
			blockOffset := offset - int64(dr.blockPos)
			if blockOffset > 0 {
				n, err := dr.buf.Seek(blockOffset, io.SeekStart)
				if err != nil || err != io.EOF {
					return err
				}
				if n != blockOffset {
					return fmt.Errorf("incorrect offset without err: %s must be %s", n, blockOffset)
				}
			}
			return nil
		case ftpb.Data_Raw:
			data := pb.GetData()
			blockOffset := uint64(offset) - dr.blockPos
			if uint64(len(data)) >= blockOffset {
				data = data[blockOffset:]
			} else {
				return nil
			}
			tailSize := dr.blockSize - blockOffset
			if uint64(len(data)) > tailSize {
				data = data[:tailSize]
			}
			dr.buf = NewBufDagReader(data)
			return nil
		case ftpb.Data_Metadata:
			return errors.New("shouldnt have had metadata object inside file")
		case ftpb.Data_Symlink:
			return errors.New("shouldnt have had symlink inside file")
		default:
			return ft.ErrUnrecognizedType
		}
	default:
		var err error
		dr.buf, err = NewDagReader(ctx, nxt, dr.serv)
		return err
	}
}

func getLinkCids(n ipld.Node) []*cid.Cid {
	links := n.Links()
	out := make([]*cid.Cid, 0, len(links))
	for _, l := range links {
		out = append(out, l.Cid)
	}
	return out
}

// Size return the total length of the data from the DAG structured file.
func (dr *PBDagReader) Size() uint64 {
	return dr.pbdata.GetFilesize()
}

// Read reads data from the DAG structured file
func (dr *PBDagReader) Read(b []byte) (int, error) {
	return dr.CtxReadFull(dr.ctx, b)
}

// CtxReadFull reads data from the DAG structured file
func (dr *PBDagReader) CtxReadFull(ctx context.Context, b []byte) (int, error) {
	if dr.buf == nil {
		if err := dr.precalcNextBuf(ctx, dr.offset); err != nil {
			return 0, err
		}
	}

	// If no cached buffer, load one
	total := 0
	for {
		bs := b[total:]
		if uint64(dr.offset) < dr.blockPos || uint64(dr.offset) > dr.blockPos + dr.blockSize {
			return total, errors.New("wrong block set")
		}
		dataSize := dr.blockSize - (uint64(dr.offset) - dr.blockPos)
		if uint64(len(bs)) > dataSize {
			bs = bs[:dataSize]
		}
		offset := 0
		// If block not sparse
		if dr.buf != nil {
			n, err := dr.buf.Read(bs)
			offset = n
			total += n
			dr.offset += int64(n)
			if err != nil {
				// EOF is expected
				if err != io.EOF {
					return total, err
				}
			}
		}

		// If weve read enough bytes, return
		if total == len(b) {
			return total, nil
		}

		// If not enough data in block
		if uint64(offset) < dataSize {
			// Fill block with zeros
			bs = bs[offset:]
			for index := range bs{
				bs[index] = 0
			}
			total += len(bs)
			dr.offset += int64(len(bs))
		}

		// If weve read enough bytes, return
		if total == len(b) {
			return total, nil
		}

		// Otherwise, load up the next block
		err := dr.precalcNextBuf(ctx, dr.offset)
		if err != nil {
			return total, err
		}
	}
}

// WriteTo writes to the given writer.
func (dr *PBDagReader) WriteTo(w io.Writer) (int64, error) {
	if dr.buf == nil {
		if err := dr.precalcNextBuf(dr.ctx, dr.offset); err != nil {
			return 0, err
		}
	}
	zerosBuf := make([]byte, 32768)

	// If no cached buffer, load one
	total := int64(0)
	for {
		// Attempt to write bytes from cached buffer
		if uint64(dr.offset) < dr.blockPos || uint64(dr.offset) > dr.blockPos + dr.blockSize {
			return total, errors.New("wrong block set")
		}
		dataSize := dr.blockSize - (uint64(dr.offset) - dr.blockPos)
		offset := int64(0)
		if dr.buf != nil {
			n, err := io.CopyN(w, dr.buf, int64(dataSize))
			offset = n
			total += n
			dr.offset += n
			if err != nil {
				if err != io.EOF {
					return total, err
				}
			}
		}
		
		// If not enough data
		if uint64(offset) < dataSize {
			// Append block with zeros
			dataSize := dataSize - uint64(offset)
			zerosBufSlice := zerosBuf
			if dataSize < uint64(len(zerosBufSlice)) {
				zerosBufSlice = zerosBufSlice[:dataSize]
			}
			for n, err := w.Write(zerosBufSlice);; n, err = w.Write(zerosBufSlice) {
				total += int64(n)
				dr.offset += int64(n)
				dataSize -= uint64(n)
				if err != nil {
					return total, err
				}
				if dataSize == 0 {
					break
				}
				if dataSize < uint64(len(zerosBufSlice)) {
					zerosBufSlice = zerosBufSlice[:dataSize]
				}
			}
		}

		// Otherwise, load up the next block
		err := dr.precalcNextBuf(dr.ctx, dr.offset)
		if err != nil {
			if err == io.EOF {
				return total, nil
			}
			return total, err
		}
	}
}

// Close closes the reader.
func (dr *PBDagReader) Close() error {
	dr.cancel()
	return nil
}

// Offset returns the current reader offset
func (dr *PBDagReader) Offset() int64 {
	return dr.offset
}

// Seek implements io.Seeker, and will seek to a given offset in the file
// interface matches standard unix seek
// TODO: check if we can do relative seeks, to reduce the amount of dagreader
// recreations that need to happen.
func (dr *PBDagReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return -1, errors.New("Invalid offset")
		}
		if offset == dr.offset {
			return offset, nil
		}
		err := dr.precalcNextBuf(dr.ctx, offset)
		if err == io.EOF {
			return dr.offset, nil
		}
		return dr.offset, err
	case io.SeekCurrent:
		// TODO: be smarter here
		if offset == 0 {
			return dr.offset, nil
		}

		noffset := dr.offset + offset
		return dr.Seek(noffset, io.SeekStart)
	case io.SeekEnd:
		noffset := int64(dr.pbdata.GetFilesize()) - offset
		n, err := dr.Seek(noffset, io.SeekStart)

		// Return negative number if we can't figure out the file size. Using io.EOF
		// for this seems to be good(-enough) solution as it's only returned by
		// precalcNextBuf when we step out of file range.
		// This is needed for gateway to function properly
		if err == io.EOF && *dr.pbdata.Type == ftpb.Data_File {
			return -1, nil
		}
		return n, err
	default:
		return 0, errors.New("invalid whence")
	}
}
