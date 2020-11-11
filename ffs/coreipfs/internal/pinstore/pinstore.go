package pinstore

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/textileio/powergate/ffs"
)

var (
	pinBaseKey = datastore.NewKey("pins")
)

type Store struct {
	lock  sync.Mutex
	ds    datastore.TxnDatastore
	cache map[cid.Cid]PinnedCid
}

// PinnedCid contains information about a pinned
// Cid from multiple APIIDs.
type PinnedCid struct {
	Cid  cid.Cid
	Pins []Pin
}

// Pin describes a pin of a Cid from a
// APIID.
type Pin struct {
	APIID     ffs.APIID
	Stage     bool
	CreatedAt int64
}

func New(ds datastore.TxnDatastore) (*Store, error) {
	cache, err := populateCache(ds)
	if err != nil {
		return nil, fmt.Errorf("populating cache: %s", err)
	}
	return &Store{ds: ds, cache: cache}, nil
}

func (s *Store) AddStaged(iid ffs.APIID, c cid.Cid) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	var r PinnedCid
	if cr, ok := s.cache[c]; ok {
		r = cr
	} else {
		r = PinnedCid{Cid: c}
	}

	for _, p := range r.Pins {
		if p.APIID == iid {
			// Looks like the APIID had this Cid
			// pinned with Hot Storage, and later
			// decided to stage the Cid again.
			// Don't mark this Cid as stage-pin since
			// that would be wrong; keep the strong pin.
			// This Cid isn't GCable.
			return nil
		}
	}

	p := Pin{
		APIID:     iid,
		Stage:     true,
		CreatedAt: time.Now().Unix(),
	}
	r.Pins = append(r.Pins, p)

	return s.persist(r)
}

func (s *Store) Add(iid ffs.APIID, c cid.Cid) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	var r PinnedCid
	if cr, ok := s.cache[c]; ok {
		r = cr
	} else {
		r = PinnedCid{Cid: c}
	}

	var p *Pin
	for i := range r.Pins {
		if r.Pins[i].APIID == iid {
			p = &r.Pins[i]
			break
		}
	}
	if p == nil {
		r.Pins = append(r.Pins, Pin{})
		p = &r.Pins[len(r.Pins)-1]
	}
	*p = Pin{
		APIID:     iid,
		Stage:     false,
		CreatedAt: time.Now().Unix(),
	}

	return s.persist(r)
}

func (s *Store) RefCount(c cid.Cid) (int, int) {
	s.lock.Lock()
	defer s.lock.Unlock()

	r, ok := s.cache[c]
	if !ok {
		return 0, 0
	}

	var stagedPins int
	for _, p := range r.Pins {
		if p.Stage {
			stagedPins++
		}
	}

	return len(r.Pins), stagedPins
}

func (s *Store) IsPinned(iid ffs.APIID, c cid.Cid) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	r, ok := s.cache[c]
	if !ok {
		return false
	}

	for _, p := range r.Pins {
		if p.APIID == iid {
			return true
		}
	}
	return false
}

func (s *Store) IsPinnedInNode(c cid.Cid) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, ok := s.cache[c]
	return ok
}

// persist persists a PinnedCid in the datastore.
func (s *Store) persist(r PinnedCid) error {
	buf, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("marshaling to datastore: %s", err)
	}
	if err := s.ds.Put(makeKey(r.Cid), buf); err != nil {
		return fmt.Errorf("put in datastore: %s", err)
	}
	s.cache[r.Cid] = r

	return nil
}

func populateCache(ds datastore.TxnDatastore) (map[cid.Cid]PinnedCid, error) {
	q := query.Query{Prefix: pinBaseKey.String()}
	res, err := ds.Query(q)
	if err != nil {
		return nil, fmt.Errorf("executing query: %s", err)
	}
	defer res.Close()

	ret := map[cid.Cid]PinnedCid{}
	for res := range res.Next() {
		if res.Error != nil {
			return nil, fmt.Errorf("query item result: %s", err)
		}
		var pc PinnedCid
		if err := json.Unmarshal(res.Value, &pc); err != nil {
			return nil, fmt.Errorf("unmarshaling result: %s", err)
		}
		ret[pc.Cid] = pc
	}
	return ret, nil
}

/*
func (s *Store) RemoveStaged(c cid.Cid) error {
	key := makeStagedKey(c)
	if err := s.ds.Delete(key); err != nil {
		return fmt.Errorf("delete in datastore: %s", err)
	}
	return nil
}

func (s *Store) GetAllStaged() ([]PinnedCid, error) {
	txn, err := s.ds.NewTransaction(true)
	if err != nil {
		return nil, fmt.Errorf("creating txn: %s", err)
	}
	defer txn.Discard()

	q := query.Query{Prefix: stagedBase.String()}
	res, err := txn.Query(q)
	if err != nil {
		return nil, fmt.Errorf("executing query: %s", err)
	}
	defer res.Close()

	var ret []PinnedCid
	for res := range res.Next() {
		if res.Error != nil {
			return nil, fmt.Errorf("query item result: %s", err)
		}
		var pc PinnedCid
		if err := json.Unmarshal(res.Value, &pc); err != nil {
			return nil, fmt.Errorf("unmarshaling result: %s", err)
		}
		ret = append(ret, pc)
	}

	return ret, nil
}
*/

func makeKey(c cid.Cid) datastore.Key {
	return pinBaseKey.ChildString(c.String())
}

/*
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
*/
