package merkledag

import (
	"gx/ipfs/QmXxGS5QsUxpR3iqL5DjmsYPHR1Yz74siRQ4ChJqWFosMh/go-block-format"

	node "gx/ipfs/QmPAKbSsgEX5B6fpmxa61jXYnoWzZr5sNafd3qgPiSH8Uv/go-ipld-format"
	u "gx/ipfs/QmWbjfz3u6HkAdPh34dgPchGbQjob6LXLhAeCGii2TX69n/go-ipfs-util"
	cid "gx/ipfs/Qma4RJSuh7mMeJQYCqMbKzekn6EwBo7HEs5AQYjVRMQATB/go-cid"
)

type RawNode struct {
	blocks.Block
}

// NewRawNode creates a RawNode using the default sha2-256 hash
// funcition.
func NewRawNode(data []byte) *RawNode {
	h := u.Hash(data)
	c := cid.NewCidV1(cid.Raw, h)
	blk, _ := blocks.NewBlockWithCid(data, c)

	return &RawNode{blk}
}

// NewRawNodeWPrefix creates a RawNode with the hash function
// specified in prefix.
func NewRawNodeWPrefix(data []byte, prefix cid.Prefix) (*RawNode, error) {
	prefix.Codec = cid.Raw
	if prefix.Version == 0 {
		prefix.Version = 1
	}
	c, err := prefix.Sum(data)
	if err != nil {
		return nil, err
	}
	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return nil, err
	}
	return &RawNode{blk}, nil
}

func (rn *RawNode) Links() []*node.Link {
	return nil
}

func (rn *RawNode) ResolveLink(path []string) (*node.Link, []string, error) {
	return nil, nil, ErrLinkNotFound
}

func (rn *RawNode) Resolve(path []string) (interface{}, []string, error) {
	return nil, nil, ErrLinkNotFound
}

func (rn *RawNode) Tree(p string, depth int) []string {
	return nil
}

func (rn *RawNode) Copy() node.Node {
	copybuf := make([]byte, len(rn.RawData()))
	copy(copybuf, rn.RawData())
	nblk, err := blocks.NewBlockWithCid(rn.RawData(), rn.Cid())
	if err != nil {
		// programmer error
		panic("failure attempting to clone raw block: " + err.Error())
	}

	return &RawNode{nblk}
}

func (rn *RawNode) Size() (uint64, error) {
	return uint64(len(rn.RawData())), nil
}

func (rn *RawNode) Stat() (*node.NodeStat, error) {
	return &node.NodeStat{
		CumulativeSize: len(rn.RawData()),
		DataSize:       len(rn.RawData()),
	}, nil
}

var _ node.Node = (*RawNode)(nil)
