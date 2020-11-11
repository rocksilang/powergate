package coreipfs

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ipfsfiles "github.com/ipfs/go-ipfs-files"
	logging "github.com/ipfs/go-log/v2"
	iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/powergate/ffs"
	"github.com/textileio/powergate/ffs/coreipfs/internal/pinstore"
	txndstr "github.com/textileio/powergate/txndstransform"
)

var (
	log = logging.Logger("ffs-coreipfs")
)

// CoreIpfs is an implementation of HotStorage interface which saves data
// into a remote go-ipfs using the HTTP API.
type CoreIpfs struct {
	ipfs iface.CoreAPI
	l    ffs.JobLogger
	ps   *pinstore.Store

	lock   sync.Mutex
	pinset map[cid.Cid]struct{}
}

var _ ffs.HotStorage = (*CoreIpfs)(nil)

// New returns a new CoreIpfs instance.
func New(ds datastore.TxnDatastore, ipfs iface.CoreAPI, l ffs.JobLogger) (*CoreIpfs, error) {
	ps, err := pinstore.New(txndstr.Wrap(ds, "pinstore"))
	if err != nil {
		return nil, fmt.Errorf("loading stroage jobstore: %s", err)
	}
	ci := &CoreIpfs{
		ipfs: ipfs,
		l:    l,
		ps:   ps,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	if err := ci.fillPinsetCache(ctx); err != nil {
		return nil, err
	}
	return ci, nil
}

// Unpin unpins a cid.
func (ci *CoreIpfs) Unpin(ctx context.Context, c cid.Cid) error {
	return ci.unpin(ctx, c)
}

// IsPinned return true if a particular Cid is pinned, both for
// staged or stored cases.
func (ci *CoreIpfs) IsPinned(ctx context.Context, c cid.Cid) (bool, error) {
	_, ok := ci.pinset[c]
	return ok, nil
}

// Stage adds an io.Reader data as file in the IPFS node. The Cid isn't considered fully
// pinned, and may be unpined by GCStaged().
func (ci *CoreIpfs) Stage(ctx context.Context, r io.Reader) (cid.Cid, error) {
	log.Debugf("adding data-stream...")
	p, err := ci.ipfs.Unixfs().Add(ctx, ipfsfiles.NewReaderFile(r), options.Unixfs.Pin(true))
	if err != nil {
		return cid.Undef, fmt.Errorf("adding data to ipfs: %s", err)
	}

	if err := ci.ps.AddStaged(p.Cid()); err != nil {
		return cid.Cid{}, fmt.Errorf("adding transient pin: %s", err)
	}
	ci.lock.Lock()
	ci.pinset[p.Cid()] = struct{}{}
	ci.lock.Unlock()
	log.Debugf("data-stream added with cid %s", p.Cid())
	return p.Cid(), nil
}

// Get retrieves a cid from the IPFS node.
func (ci *CoreIpfs) Get(ctx context.Context, c cid.Cid) (io.Reader, error) {
	log.Debugf("getting cid %s", c)
	n, err := ci.ipfs.Unixfs().Get(ctx, path.IpfsPath(c))
	if err != nil {
		return nil, fmt.Errorf("getting cid %s from ipfs: %s", c, err)
	}
	file := ipfsfiles.ToFile(n)
	if file == nil {
		return nil, fmt.Errorf("node is a directory")
	}
	return file, nil
}

// Pin a cid in the underlying storage. If the cid was already pinned by a stage,
// the Cid is considered fully-pinned.
func (ci *CoreIpfs) Pin(ctx context.Context, c cid.Cid) (int, error) {
	log.Debugf("fetching and pinning cid %s", c)
	p := path.IpfsPath(c)
	if err := ci.ipfs.Pin().Add(ctx, p, options.Pin.Recursive(true)); err != nil {
		return 0, fmt.Errorf("pinning cid %s: %s", c, err)
	}
	s, err := ci.ipfs.Object().Stat(ctx, p)
	if err != nil {
		return 0, fmt.Errorf("getting stats of cid %s: %s", c, err)
	}
	if err := ci.ps.RemoveStaged(c); err != nil {
		return 0, fmt.Errorf("deleting transient pin: %s", err)
	}
	ci.lock.Lock()
	ci.pinset[c] = struct{}{}
	ci.lock.Unlock()
	return s.CumulativeSize, nil
}

// Replace moves the pin from c1 to c2. If c2 was already pinned from a stage,
// it's considered fully-pinned.
func (ci *CoreIpfs) Replace(ctx context.Context, c1 cid.Cid, c2 cid.Cid) (int, error) {
	p1 := path.IpfsPath(c1)
	p2 := path.IpfsPath(c2)
	log.Debugf("updating pin from %s to %s", p1, p2)
	if err := ci.ipfs.Pin().Update(ctx, p1, p2); err != nil {
		return 0, fmt.Errorf("updating pin %s to %s: %s", c1, c2, err)
	}
	if err := ci.ps.RemoveStaged(c2); err != nil {
		return 0, fmt.Errorf("removing transient pin: %s", err)
	}
	stat, err := ci.ipfs.Object().Stat(ctx, p2)
	if err != nil {
		return 0, fmt.Errorf("getting stats of cid %s: %s", c2, err)
	}
	ci.lock.Lock()
	delete(ci.pinset, c1)
	ci.pinset[c2] = struct{}{}
	ci.lock.Unlock()

	return stat.CumulativeSize, nil
}

func (ci *CoreIpfs) GCStaged(ctx context.Context, exclude []cid.Cid, olderThan time.Time) ([]cid.Cid, error) {
	unpinLst, err := ci.getGCCandidates(ctx, exclude, olderThan)
	if err != nil {
		return nil, fmt.Errorf("getting gc cid candidates: %s", err)
	}
	for _, c := range unpinLst {
		if err := ci.unpin(ctx, c); err != nil {
			return nil, fmt.Errorf("unpinning cid from ipfs node: %s", err)
		}
	}

	return unpinLst, nil
}

func (ci *CoreIpfs) getGCCandidates(ctx context.Context, exclude []cid.Cid, olderThan time.Time) ([]cid.Cid, error) {
	lst, err := ci.ps.GetAllStaged()
	if err != nil {
		return nil, fmt.Errorf("get staged pins: %s", err)
	}

	excludeMap := map[cid.Cid]struct{}{}
	for _, c := range exclude {
		excludeMap[c] = struct{}{}
	}

	var unpinList []cid.Cid
	for _, stagedPin := range lst {
		if stagedPin.CreatedAt > olderThan.Unix() {
			continue
		}
		if _, ok := excludeMap[stagedPin.Cid]; ok {
			log.Infof("skipping staged cid %s since it's in exclusion list", stagedPin)
			continue
		}
		unpinList = append(unpinList, stagedPin.Cid)
	}

	return unpinList, nil
}

func (ci *CoreIpfs) unpin(ctx context.Context, c cid.Cid) error {
	if err := ci.ipfs.Pin().Rm(ctx, path.IpfsPath(c), options.Pin.RmRecursive(true)); err != nil {
		return fmt.Errorf("unpinning cid from ipfs node: %s", err)
	}
	if err := ci.ps.RemoveStaged(c); err != nil {
		return fmt.Errorf("deleting transient pin: %s", err)
	}

	ci.lock.Lock()
	delete(ci.pinset, c)
	ci.lock.Unlock()

	return nil
}

func (ci *CoreIpfs) fillPinsetCache(ctx context.Context) error {
	pins, err := ci.ipfs.Pin().Ls(ctx, options.Pin.Ls.Recursive())
	if err != nil {
		return fmt.Errorf("getting pins from IPFS: %s", err)
	}
	ci.lock.Lock()
	defer ci.lock.Unlock()
	ci.pinset = make(map[cid.Cid]struct{}, len(pins))
	for p := range pins {
		ci.pinset[p.Path().Cid()] = struct{}{}
	}
	return nil
}
