package pinstore

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

var (
	stagedBase = datastore.NewKey("staged")
)

type Store struct {
	lock sync.Mutex
	ds   datastore.TxnDatastore
}

// PinnedCid describe a pinned Cid. The Unpinnable
// field indicates if this Cid should be considered
// to be garbage collected if required.
type PinnedCid struct {
	Cid       cid.Cid
	CreatedAt int64
}

func New(ds datastore.TxnDatastore) (*Store, error) {
	return &Store{ds: ds}, nil
}

func (s *Store) AddStaged(c cid.Cid) error {
	r := PinnedCid{Cid: c, CreatedAt: time.Now().Unix()}

	buf, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("marshaling to datastore: %s", err)
	}

	key := makeStagedKey(c)
	if err := s.ds.Put(key, buf); err != nil {
		return fmt.Errorf("put in datastore: %s", err)
	}

	return nil
}

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

func makeStagedKey(c cid.Cid) datastore.Key {
	return stagedBase.ChildString(c.String())
}
