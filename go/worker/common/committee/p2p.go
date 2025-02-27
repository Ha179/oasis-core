package committee

import (
	"context"
	"time"

	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
)

type txMsgHandler struct {
	n *Node
}

func (h *txMsgHandler) DecodeMessage(msg []byte) (interface{}, error) {
	var tx []byte
	if err := cbor.Unmarshal(msg, &tx); err != nil {
		return nil, err
	}
	return tx, nil
}

func (h *txMsgHandler) AuthorizeMessage(ctx context.Context, peerID signature.PublicKey, msg interface{}) error {
	// Everyone is allowed to publish transactions.
	return nil
}

func (h *txMsgHandler) HandleMessage(ctx context.Context, peerID signature.PublicKey, msg interface{}, isOwn bool) error {
	tx := msg.([]byte) // Ensured by DecodeMessage.

	// Dispatch to any transaction handlers.
	for _, hooks := range h.n.hooks {
		err := hooks.HandlePeerTx(ctx, tx)
		if err != nil {
			return err
		}
	}
	return nil
}

// PublishTx publishes a transaction via P2P gossipsub.
func (n *Node) PublishTx(ctx context.Context, tx []byte) error {
	n.P2P.PublishTx(ctx, n.Runtime.ID(), tx)
	return nil
}

// GetMinRepublishInterval returns the minimum republish interval that needs to be respected by
// the caller when publishing the same message. If Publish is called for the same message more
// quickly, the message may be dropped and not published.
func (n *Node) GetMinRepublishInterval() time.Duration {
	return n.P2P.GetMinRepublishInterval()
}
