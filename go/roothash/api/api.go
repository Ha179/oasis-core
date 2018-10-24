// Package api implements the root hash backend API and common datastructures.
package api

import (
	"encoding"
	"encoding/hex"
	"errors"

	"golang.org/x/net/context"

	"github.com/oasislabs/ekiden/go/common/cbor"
	"github.com/oasislabs/ekiden/go/common/crypto/hash"
	"github.com/oasislabs/ekiden/go/common/crypto/signature"
	"github.com/oasislabs/ekiden/go/common/pubsub"
	"github.com/oasislabs/ekiden/go/roothash/api/block"

	pbRoothash "github.com/oasislabs/ekiden/go/grpc/roothash"
)

const (
	// HashSize is the size of the various hashes in bytes.
	HashSize = 32
)

var (
	// ErrMalformedHash is the error returned when a hash is malformed.
	ErrMalformedHash = errors.New("roothash: malformed hash")

	// ErrInvalidArgument is the error returned on malformed argument(s).
	ErrInvalidArgument = errors.New("roothash: invalid argument")

	errNilProtobuf = errors.New("roothash: protobuf is nil")

	_ encoding.BinaryMarshaler   = (*OpaqueCommitment)(nil)
	_ encoding.BinaryUnmarshaler = (*OpaqueCommitment)(nil)
	_ cbor.Marshaler             = (*DiscrepancyDetectedEvent)(nil)
	_ cbor.Unmarshaler           = (*DiscrepancyDetectedEvent)(nil)
)

// OpaqueCommitment is an opaque commitment from a compute node.
type OpaqueCommitment struct {
	// Data is the opaque commitment.
	Data []byte
}

// MarshalBinary encodes an opaque commitment into binary form.
func (c *OpaqueCommitment) MarshalBinary() (data []byte, err error) {
	data = append([]byte{}, c.Data...)
	return
}

// UnmarshalBinary decodes a binary marshaled opaque commitment.
func (c *OpaqueCommitment) UnmarshalBinary(data []byte) error {
	c.Data = append([]byte{}, data...)

	return nil
}

// FromProto deserializes a protobuf into an opaque commitment.
func (c *OpaqueCommitment) FromProto(pb *pbRoothash.Commitment) error {
	if pb == nil {
		return errNilProtobuf
	}

	return c.UnmarshalBinary(pb.GetData())
}

// ToProto serializes an opaque commitment into a protobuf.
func (c *OpaqueCommitment) ToProto() *pbRoothash.Commitment {
	pb := new(pbRoothash.Commitment)

	pb.Data, _ = c.MarshalBinary()

	return pb
}

// String returns a string representation of the opaque commitment.
func (c *OpaqueCommitment) String() string {
	return hex.EncodeToString(c.Data)
}

// Backend is a root hash consensus implementation.
type Backend interface {
	// GetLatestBlock returns the latest block.
	//
	// The metadata contained in this block can be further used to get
	// the latest state from the storage backend.
	GetLatestBlock(context.Context, signature.PublicKey) (*block.Block, error)

	// WatchBlocks returns a channel that produces a stream of blocks.
	//
	// The latest block if any will get pushed to the stream immediately.
	// Subsequent blocks will be pushed into the stream as they are
	// confirmed.
	WatchBlocks(signature.PublicKey) (<-chan *block.Block, *pubsub.Subscription, error)

	// WatchBlocksSince returns a channel that produces a stream of blocks
	// starting at the specified round.
	//
	// The block at the specified round is included as the first
	// entry in the stream.  Following blocks are pushed in order as
	// they are confirmed.
	WatchBlocksSince(signature.PublicKey, block.Round) (<-chan *block.Block, *pubsub.Subscription, error)

	// WatchEvents returns a stream of protocol events.
	WatchEvents(signature.PublicKey) (<-chan *Event, *pubsub.Subscription, error)

	// Commit commits to a result of processing a batch of runtime invocations.
	Commit(context.Context, signature.PublicKey, *OpaqueCommitment) error

	// Cleanup cleans up the roothash backend.
	Cleanup()
}

// DiscrepancyDetectedEvent is a discrepancy detected event.
type DiscrepancyDetectedEvent struct {
	// BatchHash is the CallBatch hash that is set when a discrepancy
	// is detected to signal to the backup workers that a computation
	// should be re-executed.
	BatchHash *hash.Hash `codec:"batch_hash"`

	// BlockHeader is the block header of the block on which the backup
	// computation should be based.
	BlockHeader *block.Header `codec:"header"`
}

// MarshalCBOR serializes the type into a CBOR byte vector.
func (e *DiscrepancyDetectedEvent) MarshalCBOR() []byte {
	return cbor.Marshal(e)
}

// UnmarshalCBOR decodes a CBOR marshaled block.
func (e *DiscrepancyDetectedEvent) UnmarshalCBOR(data []byte) error {
	return cbor.Unmarshal(data, e)
}

// Event is a protocol event.
type Event struct {
	// DiscrepancyDetected is the CallBatch hash that is set when a
	// discrepancy is detected to signal to the backup workers that a
	// computation should be re-executed.
	DiscrepancyDetected *DiscrepancyDetectedEvent
}

// MetricsMonitorable is the interface exposed by backends capable of
// providing metrics data.
type MetricsMonitorable interface {
	// WatchAllBlocks returns a channel that produces a stream of blocks.
	//
	// All blocks from all runtimes will be pushed into the stream
	// immediately as they are finalized.
	WatchAllBlocks() (<-chan *block.Block, *pubsub.Subscription)
}
