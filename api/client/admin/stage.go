package admin

import (
	"context"

	proto "github.com/textileio/powergate/proto/admin/v1"
)

// Data provides access to Powergate data admin APIs.
type Data struct {
	client proto.PowergateAdminServiceClient
}

// GCStaged unpins staged data not related to queued or executing jobs.
func (w *Data) GCStaged(ctx context.Context) (*proto.GCStagedResponse, error) {
	return w.client.GCStaged(ctx, &proto.GCStagedRequest{})
}
