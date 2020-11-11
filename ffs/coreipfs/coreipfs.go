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
	ps   *pinstore.Store

	lock sync.Mutex
}

var _ ffs.HotStorage = (*CoreIpfs)(nil)

// New returns a new CoreIpfs instance.
func New(ds datastore.TxnDatastore, ipfs iface.CoreAPI, l ffs.JobLogger) (*CoreIpfs, error) {
	ps, err := pinstore.New(txndstr.Wrap(ds, "pinstore"))
	if err != nil {
		return nil, fmt.Errorf("loading pinstore: %s", err)
	}
	ci := &CoreIpfs{
		ipfs: ipfs,
		ps:   ps,
	}
	return ci, nil
}

// Unpin unpins a Cid for an APIID.
func (ci *CoreIpfs) Unpin(ctx context.Context, iid ffs.APIID, c cid.Cid) error {
	return ci.unpin(ctx, iid, c)
}

func (ci *CoreIpfs) IsPinned(ctx context.Context, iid ffs.APIID, c cid.Cid) (bool, error) {
	return ci.ps.IsPinned(iid, c), nil
}

// Stage creates a stage-pin for a data stream for an APIID. This pin can be considered unpinnable
// automatically by GCStaged().
func (ci *CoreIpfs) Stage(ctx context.Context, iid ffs.APIID, r io.Reader) (cid.Cid, error) {
	p, err := ci.ipfs.Unixfs().Add(ctx, ipfsfiles.NewReaderFile(r), options.Unixfs.Pin(true))
	if err != nil {
		return cid.Undef, fmt.Errorf("adding data to ipfs: %s", err)
	}

	// APIID already pinned this Cid,  no ref count to update here.
	// May happen if the user is staging mutiple times
	// the same data for no reason, or staging the data
	// again after it was already pinned by Hot Storage.
	// In any case, the ref count is already counted for
	// this APIID, nothing to do.
	if ci.ps.IsPinned(iid, p.Cid()) {
		return p.Cid(), nil
	}

	if err := ci.ps.AddStaged(iid, p.Cid()); err != nil {
		return cid.Undef, fmt.Errorf("saving new pin in pinstore: %s", err)
	}

	return p.Cid(), nil
}

// Get retrieves a cid from the IPFS node.
func (ci *CoreIpfs) Get(ctx context.Context, c cid.Cid) (io.Reader, error) {
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

// Pin a cid for an APIID. If the cid was already pinned by a stage from APIID,
// the Cid is considered fully-pinned and not a candidate to be unpinned by GCStaged().
func (ci *CoreIpfs) Pin(ctx context.Context, iid ffs.APIID, c cid.Cid) (int, error) {
	p := path.IpfsPath(c)

	// If some APIID already pinned this Cid in the underlying go-ipfs node, then
	// we don't need to call the Pin API, just count the reference from this APIID.
	if !ci.ps.IsPinnedInNode(c) {
		if err := ci.ipfs.Pin().Add(ctx, p, options.Pin.Recursive(true)); err != nil {
			return 0, fmt.Errorf("pinning cid %s: %s", c, err)
		}
	}
	s, err := ci.ipfs.Object().Stat(ctx, p)
	if err != nil {
		return 0, fmt.Errorf("getting stats of cid %s: %s", c, err)
	}

	// Count +1 reference to this Cid by APIID.
	if err := ci.ps.Add(iid, p.Cid()); err != nil {
		return 0, fmt.Errorf("saving new pin in pinstore: %s", err)
	}

	return s.CumulativeSize, nil
}

// Replace moves the pin from c1 to c2. If c2 was already pinned from a stage,
// it's considered fully-pinned.
func (ci *CoreIpfs) Replace(ctx context.Context, iid ffs.APIID, c1 cid.Cid, c2 cid.Cid) (int, error) {
	p1 := path.IpfsPath(c1)
	p2 := path.IpfsPath(c2)

	c1refcount, _ := ci.ps.RefCount(c1)
	c2refcount, _ := ci.ps.RefCount(c2)

	if c1refcount == 0 {
		return 0, fmt.Errorf("c1 pin from replace isn't pinned")
	}

	// If c1 has a single reference, which must be from iid, and c2 isn't pinned
	// then move the pin, which is the fastest way to unpin and pin two cids that might
	// share part of the dag.
	if c1refcount == 1 && c2refcount == 0 {
		if err := ci.ipfs.Pin().Update(ctx, p1, p2); err != nil {
			return 0, fmt.Errorf("updating pin %s to %s: %s", c1, c2, err)
		}
	} else if c2refcount == 0 {
		// - c1 is pinned by another iid, so we can't unpin it.
		// - c2 isn't pinned by anyone, so we should pin it.
		if err := ci.ipfs.Pin().Add(ctx, p2, options.Pin.Recursive(true)); err != nil {
			return 0, fmt.Errorf("pinning cid %s: %s", c2, err)
		}
	} else {
		// - c1 is pinned by another iid, so we can't unpin it.
		// - c2 is pinned by some other iid, so it's already pinned in the node, no need to do it.
	}

	// In any case of above if, update the ref counts.

	stat, err := ci.ipfs.Object().Stat(ctx, p2)
	if err != nil {
		return 0, fmt.Errorf("getting stats of cid %s: %s", c2, err)
	}

	// Decrease for iid refcount by one to c1, and increase by one to c2.
	if err := ci.ps.Remove(iid, c1); err != nil {
		return 0, fmt.Errorf("removing cid in pinstore: %s", err)
	}
	if err := ci.ps.Add(iid, c2); err != nil {
		return 0, fmt.Errorf("adding cid in pinstore: %s", err)
	}

	return stat.CumulativeSize, nil
}

// GCStaged unpins Cids that are only pinned by Stage() calls and all pins satisfy the filters.
func (ci *CoreIpfs) GCStaged(ctx context.Context, exclude []cid.Cid, olderThan time.Time) ([]cid.Cid, error) {
	unpinLst, err := ci.getGCCandidates(ctx, exclude, olderThan)
	if err != nil {
		return nil, fmt.Errorf("getting gc cid candidates: %s", err)
	}

	for _, c := range unpinLst {
		if err := ci.unpinStaged(ctx, c); err != nil {
			return nil, fmt.Errorf("unpinning cid from ipfs node: %s", err)
		}
	}

	return unpinLst, nil
}

func (ci *CoreIpfs) getGCCandidates(ctx context.Context, exclude []cid.Cid, olderThan time.Time) ([]cid.Cid, error) {
	lst, err := ci.ps.GetAllOnlyStaged()
	if err != nil {
		return nil, fmt.Errorf("get staged pins: %s", err)
	}

	excludeMap := map[cid.Cid]struct{}{}
	for _, c := range exclude {
		excludeMap[c] = struct{}{}
	}

	var unpinList []cid.Cid
Loop:
	for _, stagedPin := range lst {
		// Skip Cids that are excluded.
		if _, ok := excludeMap[stagedPin.Cid]; ok {
			log.Infof("skipping staged cid %s since it's in exclusion list", stagedPin)
			continue Loop
		}
		// A Cid is only safe to GC if all existing stage-pin are older than
		// specified parameter. If any iid stage-pined the Cid more recently than olderThan
		// we still have to wait a bit more to consider it for GC.
		for _, sp := range stagedPin.Pins {
			if sp.CreatedAt > olderThan.Unix() {
				continue Loop
			}

		}

		// The Cid only has staged-pins, and all iids that staged it aren't in exclusion list
		// plus are older than olderThan ==> Safe to GCed.
		unpinList = append(unpinList, stagedPin.Cid)
	}

	return unpinList, nil
}

func (ci *CoreIpfs) unpin(ctx context.Context, iid ffs.APIID, c cid.Cid) error {
	count, _ := ci.ps.RefCount(c)
	if count == 0 {
		return fmt.Errorf("cid %s for %s isn't pinned", c, iid)
	}

	if count == 1 {
		// There aren't more pinnings for this Cid, let's unpin from IPFS.
		log.Infof("unpinning cid %s with ref count 0", c)
		if err := ci.ipfs.Pin().Rm(ctx, path.IpfsPath(c), options.Pin.RmRecursive(true)); err != nil {
			return fmt.Errorf("unpinning cid from ipfs node: %s", err)
		}
	}

	if err := ci.ps.Remove(iid, c); err != nil {
		return fmt.Errorf("removing cid from pinstore: %s", err)
	}

	return nil
}

func (ci *CoreIpfs) unpinStaged(ctx context.Context, c cid.Cid) error {
	count, stagedCount := ci.ps.RefCount(c)

	// Just in case, verify that the total number of pins are equal
	// to stage-pins. That is, nobody is pinning this Cid apart from Stage() calls.
	if count != stagedCount {
		return fmt.Errorf("cid %s hasn't only stage-pins, total %d staged %d", c, count, stagedCount)
	}

	if err := ci.ipfs.Pin().Rm(ctx, path.IpfsPath(c), options.Pin.RmRecursive(true)); err != nil {
		return fmt.Errorf("unpinning cid from ipfs node: %s", err)
	}

	if err := ci.ps.RemoveStaged(c); err != nil {
		return fmt.Errorf("removing all staged pins for %s: %s", c, err)
	}

	return nil
}
