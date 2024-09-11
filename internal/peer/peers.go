package peer

import (
	"context"
	"errors"

	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/config"
)

// Peers holds the collection of peers for the cluster
type Peers interface {
	GetPeers() ([]string, error)
	GetInstanceID() (string, error)
	RegisterUpdatedPeersCallback(callback func())

	Ready() error
	// make it injectable
	startstop.Starter
}

func NewPeers(ctx context.Context, c config.Config, done chan struct{}) (Peers, error) {
	t := c.GetPeerManagementType()

	switch t {
	case "file":
		return &FilePeers{Done: done}, nil
	case "redis":
		return &RedisPubsubPeers{Done: done}, nil
	case "fly-dns":
		return NewDnsPeers(c, done)
	default:
		return nil, errors.New("invalid config option 'PeerManagement.Type'")
	}
}
