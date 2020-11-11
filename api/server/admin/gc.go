package admin

import (
	"context"

	adminProto "github.com/textileio/powergate/proto/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GCStaged runs a unpinning garbage collection and returns the unpinned cids.
func (a *Service) GCStaged(ctx context.Context, req *adminProto.GCStagedRequest) (*adminProto.GCStagedResponse, error) {
	cids, err := a.s.GCStaged(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "running FFS GC: %v", err)
	}

	cidsStr := make([]string, len(cids))
	for i := range cids {
		cidsStr[i] = cids[i].String()
	}

	return &adminProto.GCStagedResponse{
		UnpinnedCids: cidsStr,
	}, nil
}
