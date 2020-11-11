package coreipfs

import (
	"bytes"
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	httpapi "github.com/ipfs/go-ipfs-http-client"
	"github.com/stretchr/testify/require"
	it "github.com/textileio/powergate/ffs/integrationtest"
	"github.com/textileio/powergate/ffs/joblogger"
	"github.com/textileio/powergate/tests"
	txndstr "github.com/textileio/powergate/txndstransform"
)

func TestStagePinUnpin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	coreipfs, ipfs := newCoreIPFS(t)

	r := rand.New(rand.NewSource(22))
	data := it.RandomBytes(r, 1500)

	// # Stage
	c, err := coreipfs.Stage(ctx, bytes.NewReader(data))
	require.NoError(t, err)
	// Check that staged data is pinned.
	it.RequireIpfsPinnedCid(ctx, t, c, ipfs)
	// Check that staged data is GCable
	requireCidIsGCable(t, coreipfs, c)
	okPinned, err := coreipfs.IsPinned(ctx, c)
	require.NoError(t, err)
	require.True(t, okPinned)

	// # Pin
	_, err = coreipfs.Pin(ctx, c)
	require.NoError(t, err)
	// Check is still pinned.
	it.RequireIpfsPinnedCid(ctx, t, c, ipfs)
	// Check that now can't be GCed.
	requireCidIsNotGCable(t, coreipfs, c)
	okPinned, err = coreipfs.IsPinned(ctx, c)
	require.NoError(t, err)
	require.True(t, okPinned)

	// # Unpin
	err = coreipfs.Unpin(ctx, c)
	require.NoError(t, err)
	it.RequireIpfsUnpinnedCid(ctx, t, c, ipfs)
	okPinned, err = coreipfs.IsPinned(ctx, c)
	require.NoError(t, err)
	require.False(t, okPinned)
}

func TestGC(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("Simple", func(t *testing.T) {
		coreipfs, ipfs := newCoreIPFS(t)
		// # Stage 1
		r := rand.New(rand.NewSource(22))
		data := it.RandomBytes(r, 1500)
		c1, err := coreipfs.Stage(ctx, bytes.NewReader(data))
		require.NoError(t, err)

		// # Stage 1
		data = it.RandomBytes(r, 1500)
		c2, err := coreipfs.Stage(ctx, bytes.NewReader(data))
		require.NoError(t, err)

		gced, err := coreipfs.GCStaged(ctx, nil, time.Now())
		require.NoError(t, err)
		require.Len(t, gced, 2)

		it.RequireIpfsUnpinnedCid(ctx, t, c1, ipfs)
		it.RequireIpfsUnpinnedCid(ctx, t, c2, ipfs)

		gced, err = coreipfs.GCStaged(ctx, nil, time.Now())
		require.NoError(t, err)
		require.Len(t, gced, 0)
	})

	t.Run("Exclusion", func(t *testing.T) {
		coreipfs, ipfs := newCoreIPFS(t)
		// # Stage 1
		r := rand.New(rand.NewSource(22))
		data := it.RandomBytes(r, 1500)
		c1, err := coreipfs.Stage(ctx, bytes.NewReader(data))
		require.NoError(t, err)

		// # Stage 1
		data = it.RandomBytes(r, 1500)
		c2, err := coreipfs.Stage(ctx, bytes.NewReader(data))
		require.NoError(t, err)

		gced, err := coreipfs.GCStaged(ctx, []cid.Cid{c1}, time.Now())
		require.NoError(t, err)
		require.Len(t, gced, 1)

		it.RequireIpfsUnpinnedCid(ctx, t, c2, ipfs)

		gced, err = coreipfs.GCStaged(ctx, nil, time.Now())
		require.NoError(t, err)
		require.Len(t, gced, 1)
		it.RequireIpfsUnpinnedCid(ctx, t, c1, ipfs)
	})

	t.Run("Very old", func(t *testing.T) {
		coreipfs, ipfs := newCoreIPFS(t)
		// # Stage 1
		r := rand.New(rand.NewSource(22))
		data := it.RandomBytes(r, 1500)
		c1, err := coreipfs.Stage(ctx, bytes.NewReader(data))
		require.NoError(t, err)

		// # Stage 1
		data = it.RandomBytes(r, 1500)
		c2, err := coreipfs.Stage(ctx, bytes.NewReader(data))
		require.NoError(t, err)

		gced, err := coreipfs.GCStaged(ctx, nil, time.Now().Add(-time.Hour))
		require.NoError(t, err)
		require.Len(t, gced, 0)

		gced, err = coreipfs.GCStaged(ctx, nil, time.Now())
		require.NoError(t, err)
		require.Len(t, gced, 2)

		it.RequireIpfsUnpinnedCid(ctx, t, c1, ipfs)
		it.RequireIpfsUnpinnedCid(ctx, t, c2, ipfs)
	})

}

func requireCidIsGCable(t *testing.T, ci *CoreIpfs, c cid.Cid) bool {
	lst, err := ci.getGCCandidates(context.Background(), nil, time.Now())
	require.NoError(t, err)

	for _, cid := range lst {
		if cid.Equals(c) {
			return true
		}
	}
	return false
}

func requireCidIsNotGCable(t *testing.T, ci *CoreIpfs, c cid.Cid) bool {
	return !requireCidIsGCable(t, ci, c)
}

func newCoreIPFS(t *testing.T) (*CoreIpfs, *httpapi.HttpApi) {
	ds := tests.NewTxMapDatastore()
	ipfs, _ := it.CreateIPFS(t)
	l := joblogger.New(txndstr.Wrap(ds, "ffs/joblogger"))
	hl, err := New(ds, ipfs, l)
	require.NoError(t, err)

	return hl, ipfs

}
