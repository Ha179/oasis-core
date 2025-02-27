package rpc

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/cenkalti/backoff/v4"
	core "github.com/libp2p/go-libp2p-core"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/protocol"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/errors"
	"github.com/oasisprotocol/oasis-core/go/common/logging"
	"github.com/oasisprotocol/oasis-core/go/common/version"
	"github.com/oasisprotocol/oasis-core/go/common/workerpool"
)

const (
	// RequestWriteDeadline is the maximum amount of time that can be spent on writing a request.
	RequestWriteDeadline = 5 * time.Second
	// DefaultCallRetryInterval is the default call retry interval for calls which explicitly enable
	// retries by setting the WithMaxRetries option to a non-zero value. It can be overridden by
	// using the WithRetryInterval call option.
	DefaultCallRetryInterval = 1 * time.Second
)

// PeerFeedback is an interface for providing deferred peer feedback after an outcome is known.
type PeerFeedback interface {
	// RecordSuccess records a successful protocol interaction with the given peer.
	RecordSuccess()

	// RecordFailure records an unsuccessful protocol interaction with the given peer.
	RecordFailure()

	// RecordBadPeer records a malicious protocol interaction with the given peer.
	//
	// The peer will be ignored during peer selection.
	RecordBadPeer()
}

type peerFeedback struct {
	mgr     PeerManager
	peerID  core.PeerID
	latency time.Duration
}

func (pf *peerFeedback) RecordSuccess() {
	pf.mgr.RecordSuccess(pf.peerID, pf.latency)
}

func (pf *peerFeedback) RecordFailure() {
	pf.mgr.RecordFailure(pf.peerID, pf.latency)
}

func (pf *peerFeedback) RecordBadPeer() {
	pf.mgr.RecordBadPeer(pf.peerID)
}

type nopPeerFeedback struct{}

func (pf *nopPeerFeedback) RecordSuccess() {
}

func (pf *nopPeerFeedback) RecordFailure() {
}

func (pf *nopPeerFeedback) RecordBadPeer() {
}

// NewNopPeerFeedback creates a no-op peer feedback instance.
func NewNopPeerFeedback() PeerFeedback {
	return &nopPeerFeedback{}
}

// ClientOptions are client options.
type ClientOptions struct {
	stickyPeers bool
	peerFilter  PeerFilter
}

// ClientOption is a client option setter.
type ClientOption func(opts *ClientOptions)

// WithStickyPeers configures the sticky peers feature.
//
// When enabled, the last successful peer will be stuck and will be reused on subsequent calls until
// the peer is deemed bad by the received peer feedback.
func WithStickyPeers(enabled bool) ClientOption {
	return func(opts *ClientOptions) {
		opts.stickyPeers = enabled
	}
}

// PeerFilter is a peer filtering interface.
type PeerFilter interface {
	// IsPeerAcceptable checks whether the given peer should be used.
	IsPeerAcceptable(peerID core.PeerID) bool
}

// WithPeerFilter configures peer filtering.
//
// When set, only peers accepted by the filter will be used for calls.
func WithPeerFilter(filter PeerFilter) ClientOption {
	return func(opts *ClientOptions) {
		opts.peerFilter = filter
	}
}

// CallOptions are per-call options.
type CallOptions struct {
	retryInterval time.Duration
	maxRetries    uint64
}

// CallOption is a per-call option setter.
type CallOption func(opts *CallOptions)

// WithMaxRetries configures the maximum number of retries to use for the call.
func WithMaxRetries(maxRetries uint64) CallOption {
	return func(opts *CallOptions) {
		opts.maxRetries = maxRetries
	}
}

// WithRetryInterval configures the retry interval to use for the call.
func WithRetryInterval(retryInterval time.Duration) CallOption {
	return func(opts *CallOptions) {
		opts.retryInterval = retryInterval
	}
}

// Client is an RPC client for a given protocol.
type Client interface {
	PeerManager

	// Call attempts to route the given RPC method call to one of the peers that supports the
	// protocol based on past experience with the peers.
	//
	// On success it returns a PeerFeedback instance that should be used by the caller to provide
	// deferred feedback on whether the peer is any good or not. This will help guide later choices
	// when routing calls.
	Call(
		ctx context.Context,
		method string,
		body, rsp interface{},
		maxPeerResponseTime time.Duration,
		opts ...CallOption,
	) (PeerFeedback, error)

	// CallMulti routes the given RPC method call to multiple peers that support the protocol based
	// on past experience with the peers.
	//
	// It returns all successfully retrieved results and their corresponding PeerFeedback instances.
	CallMulti(
		ctx context.Context,
		method string,
		body, rspTyp interface{},
		maxPeerResponseTime time.Duration,
		maxParallelRequests uint,
	) ([]interface{}, []PeerFeedback, error)
}

type client struct {
	PeerManager

	host       core.Host
	protocolID protocol.ID
	runtimeID  common.Namespace

	opts *ClientOptions

	logger *logging.Logger
}

func (c *client) isPeerAcceptable(peerID core.PeerID) bool {
	if c.opts.peerFilter == nil {
		return true
	}

	return c.opts.peerFilter.IsPeerAcceptable(peerID)
}

func (c *client) Call(
	ctx context.Context,
	method string,
	body, rsp interface{},
	maxPeerResponseTime time.Duration,
	opts ...CallOption,
) (PeerFeedback, error) {
	c.logger.Debug("call", "method", method)

	co := CallOptions{
		retryInterval: DefaultCallRetryInterval,
	}
	for _, opt := range opts {
		opt(&co)
	}

	// Prepare the request.
	request := Request{
		Method: method,
		Body:   cbor.Marshal(body),
	}

	var pf PeerFeedback
	tryPeers := func() error {
		// Iterate through the prioritized list of peers and attempt to execute the request.
		for _, peer := range c.GetBestPeers() {
			if !c.isPeerAcceptable(peer) {
				continue
			}

			c.logger.Debug("trying peer",
				"method", method,
				"peer_id", peer,
			)

			var err error
			pf, err = c.call(ctx, peer, &request, rsp, maxPeerResponseTime)
			if err != nil {
				continue
			}
			return nil
		}

		// No peers could be reached to service this request.
		c.logger.Debug("no peers could be reached to service request",
			"method", method,
		)

		return fmt.Errorf("call failed on all peers")
	}

	var err error
	if co.maxRetries > 0 {
		retry := backoff.WithMaxRetries(backoff.NewConstantBackOff(co.retryInterval), co.maxRetries)
		err = backoff.Retry(tryPeers, backoff.WithContext(retry, ctx))
	} else {
		err = tryPeers()
	}

	return pf, err
}

func (c *client) CallMulti(
	ctx context.Context,
	method string,
	body, rspTyp interface{},
	maxPeerResponseTime time.Duration,
	maxParallelRequests uint,
) ([]interface{}, []PeerFeedback, error) {
	c.logger.Debug("call multiple", "method", method)

	// Prepare the request.
	request := Request{
		Method: method,
		Body:   cbor.Marshal(body),
	}

	// Create a worker pool.
	pool := workerpool.New("p2p/rpc")
	pool.Resize(maxParallelRequests)
	defer pool.Stop()

	// Requests results from peers.
	type result struct {
		rsp interface{}
		pf  PeerFeedback
		err error
	}
	var resultCh []chan *result
	for _, peer := range c.GetBestPeers() {
		if !c.isPeerAcceptable(peer) {
			continue
		}

		ch := make(chan *result, 1)
		resultCh = append(resultCh, ch)

		pool.Submit(func() {
			rsp := reflect.New(reflect.TypeOf(rspTyp)).Interface()
			pf, err := c.call(ctx, peer, &request, rsp, maxPeerResponseTime)
			ch <- &result{rsp, pf, err}
			close(ch)
		})
	}

	// Gather results.
	var (
		rsps []interface{}
		pfs  []PeerFeedback
	)
	for _, ch := range resultCh {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case result := <-ch:
			// Ignore failed results.
			if result.err != nil {
				continue
			}

			rsps = append(rsps, result.rsp)
			pfs = append(pfs, result.pf)
		}
	}
	return rsps, pfs, nil
}

func (c *client) call(
	ctx context.Context,
	peerID core.PeerID,
	request *Request,
	rsp interface{},
	maxPeerResponseTime time.Duration,
) (PeerFeedback, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	startTime := time.Now()

	err := c.sendRequestAndDecodeResponse(ctx, peerID, request, rsp, maxPeerResponseTime)
	if err != nil {
		c.logger.Debug("failed to call method",
			"err", err,
			"method", request.Method,
			"peer_id", peerID,
		)

		c.RecordFailure(peerID, time.Since(startTime))
		return nil, err
	}

	pf := &peerFeedback{
		mgr:     c.PeerManager,
		peerID:  peerID,
		latency: time.Since(startTime),
	}
	return pf, nil
}

func (c *client) sendRequestAndDecodeResponse(
	ctx context.Context,
	peerID core.PeerID,
	request *Request,
	rsp interface{},
	maxPeerResponseTime time.Duration,
) error {
	// Attempt to open stream to the given peer.
	stream, err := c.host.NewStream(
		network.WithNoDial(ctx, "should already have connection"),
		peerID,
		c.protocolID,
	)
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	codec := cbor.NewMessageCodec(stream, codecModuleName)

	// Send request.
	_ = stream.SetWriteDeadline(time.Now().Add(RequestWriteDeadline))
	if err = codec.Write(request); err != nil {
		c.logger.Debug("failed to send request",
			"err", err,
			"peer_id", peerID,
		)
		return fmt.Errorf("failed to send request: %w", err)
	}
	_ = stream.SetWriteDeadline(time.Time{})

	// Read response.
	// TODO: Add required minimum speed.
	var rawRsp Response
	_ = stream.SetReadDeadline(time.Now().Add(maxPeerResponseTime))
	if err = codec.Read(&rawRsp); err != nil {
		c.logger.Debug("failed to read response",
			"err", err,
			"peer_id", peerID,
		)
		return fmt.Errorf("failed to read response: %w", err)
	}
	_ = stream.SetWriteDeadline(time.Time{})

	// Decode response.
	if rawRsp.Error != nil {
		return errors.FromCode(rawRsp.Error.Module, rawRsp.Error.Code, rawRsp.Error.Message)
	}

	if rsp != nil {
		return cbor.Unmarshal(rawRsp.Ok, rsp)
	}
	return nil
}

// NewClient creates a new RPC client for the given protocol.
func NewClient(p2p P2P, runtimeID common.Namespace, protocolID string, version version.Version, opts ...ClientOption) Client {
	pid := NewRuntimeProtocolID(runtimeID, protocolID, version)

	var co ClientOptions
	for _, opt := range opts {
		opt(&co)
	}

	return &client{
		PeerManager: NewPeerManager(p2p, pid, co.stickyPeers),
		host:        p2p.GetHost(),
		protocolID:  pid,
		runtimeID:   runtimeID,
		opts:        &co,
		logger: logging.GetLogger("worker/common/p2p/rpc/client").With(
			"protocol", protocolID,
			"runtime_id", runtimeID,
		),
	}
}
