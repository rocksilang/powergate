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
	Staged    bool
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

	for i, p := range r.Pins {
		if p.APIID == iid {
			if !p.Staged {
				// Looks like the APIID had this Cid
				// pinned with Hot Storage, and later
				// decided to stage the Cid again.
				// Don't mark this Cid as stage-pin since
				// that would be wrong; keep the strong pin.
				// This Cid isn't GCable.
				return nil
			}
			// If the Cid is pinned because of a stage,
			// and is re-staged then simply update its
			// CreatedAt, so it will survive longer to a
			// GC.
			r.Pins[i].CreatedAt = time.Now().Unix()
			return s.persist(r)
		}
	}

	// If the Cid is not present, create it as a staged pin.
	p := Pin{
		APIID:     iid,
		Staged:    true,
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
		Staged:    false,
		CreatedAt: time.Now().Unix(),
	}

	return s.persist(r)
}

// RefCount returns two integers (total, staged).
// total is the total number of ref counts for the Cid.
// staged is the total number of ref counts corresponding to
// staged pins. total includes staged, this means that:
// * total >= staged
// * non-staged pins = total - staged
func (s *Store) RefCount(c cid.Cid) (int, int) {
	s.lock.Lock()
	defer s.lock.Unlock()

	r, ok := s.cache[c]
	if !ok {
		return 0, 0
	}

	var stagedPins int
	for _, p := range r.Pins {
		if p.Staged {
			stagedPins++
		}
	}

	return len(r.Pins), stagedPins
}

// IsPinnedBy returns true if the Cid is pinned for APIID.
// Both strong and staged pins are considered.
func (s *Store) IsPinnedBy(iid ffs.APIID, c cid.Cid) bool {
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

func (s *Store) IsPinned(c cid.Cid) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, ok := s.cache[c]
	return ok
}

func (s *Store) Remove(iid ffs.APIID, c cid.Cid) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	r, ok := s.cache[c]
	if !ok {
		return fmt.Errorf("c1 isn't pinned")
	}

	c1idx := -1
	for i, p := range r.Pins {
		if p.APIID == iid {
			c1idx = i
			break
		}
	}
	if c1idx == -1 {
		return nil
	}
	r.Pins[c1idx] = r.Pins[len(r.Pins)-1]
	r.Pins = r.Pins[:len(r.Pins)-1]

	return s.persist(r)
}

func (s *Store) RemoveStaged(c cid.Cid) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	pc1, ok := s.cache[c]
	if !ok {
		return fmt.Errorf("c1 isn't pinned")
	}

	for _, p := range pc1.Pins {
		if !p.Staged {
			return fmt.Errorf("all pins should be stage type")
		}
	}

	if err := s.ds.Delete(makeKey(c)); err != nil {
		return fmt.Errorf("deleting from datastore: %s", err)
	}
	s.cache[c] = pc1

	return nil
}

func (s *Store) GetAllOnlyStaged() ([]PinnedCid, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var res []PinnedCid
Loop:
	for _, v := range s.cache {
		for _, p := range v.Pins {
			if !p.Staged {
				continue Loop
			}
		}

		res = append(res, v)
	}
	return res, nil
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

func makeKey(c cid.Cid) datastore.Key {
	return pinBaseKey.ChildString(c.String())
}
