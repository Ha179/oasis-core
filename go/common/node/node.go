// Package node implements common node identity routines.
//
// This package is meant for interoperability with the rust compute worker.
package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/oasisprotocol/oasis-core/go/common"
	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/hash"
	"github.com/oasisprotocol/oasis-core/go/common/crypto/signature"
	"github.com/oasisprotocol/oasis-core/go/common/prettyprint"
	"github.com/oasisprotocol/oasis-core/go/common/sgx"
	"github.com/oasisprotocol/oasis-core/go/common/sgx/ias"
	"github.com/oasisprotocol/oasis-core/go/common/version"
)

var (
	// ErrInvalidRole is the error returned when a node role is invalid.
	ErrInvalidRole = errors.New("node: invalid role")
	// ErrDuplicateRole is the error returned when a node role is duplicated.
	ErrDuplicateRole = errors.New("node: duplicate role")

	// ErrInvalidTEEHardware is the error returned when a TEE hardware
	// implementation is invalid.
	ErrInvalidTEEHardware = errors.New("node: invalid TEE implementation")

	// ErrRAKHashMismatch is the error returned when the TEE attestation
	// does not contain the node's RAK hash.
	ErrRAKHashMismatch = errors.New("node: RAK hash mismatch")

	// ErrBadEnclaveIdentity is the error returned when the TEE enclave
	// identity doesn't match the required values.
	ErrBadEnclaveIdentity = errors.New("node: bad TEE enclave identity")

	// ErrConstraintViolation the error returned when the TEE attestation
	// fails to conform to the optional additional constraints.
	ErrConstraintViolation = errors.New("node: TEE constraint violation")

	teeHashContext = []byte("oasis-core/node: TEE RAK binding")

	_ prettyprint.PrettyPrinter = (*MultiSignedNode)(nil)
)

const (
	// LatestNodeDescriptorVersion is the latest node descriptor version that should be used for all
	// new descriptors. Using earlier versions may be rejected.
	LatestNodeDescriptorVersion = 1

	// Minimum and maximum descriptor versions that are allowed.
	minNodeDescriptorVersion = 1
	maxNodeDescriptorVersion = LatestNodeDescriptorVersion
)

// Node represents public connectivity information about an Oasis node.
type Node struct { // nolint: maligned
	cbor.Versioned

	// ID is the public key identifying the node.
	ID signature.PublicKey `json:"id"`

	// EntityID is the public key identifying the Entity controlling
	// the node.
	EntityID signature.PublicKey `json:"entity_id"`

	// Expiration is the epoch in which this node's commitment expires.
	Expiration uint64 `json:"expiration"`

	// TLS contains information for connecting to this node via TLS.
	TLS TLSInfo `json:"tls"`

	// P2P contains information for connecting to this node via P2P.
	P2P P2PInfo `json:"p2p"`

	// Consensus contains information for connecting to this node as a
	// consensus member.
	Consensus ConsensusInfo `json:"consensus"`

	// VRF contains information for this node's participation in VRF
	// based elections.
	VRF *VRFInfo `json:"vrf,omitempty"`

	// DeprecatedBeacon contains information for this node's
	// participation in the old PVSS based random beacon protocol.
	DeprecatedBeacon cbor.RawMessage `json:"beacon,omitempty"`

	// Runtimes are the node's runtimes.
	Runtimes []*Runtime `json:"runtimes"`

	// Roles is a bitmask representing the node roles.
	Roles RolesMask `json:"roles"`

	// SoftwareVersion is the node's oasis-node software version.
	SoftwareVersion string `json:"software_version,omitempty"`
}

// RolesMask is Oasis node roles bitmask.
type RolesMask uint32

const (
	// RoleComputeWorker is the compute worker role.
	RoleComputeWorker RolesMask = 1 << 0
	// RoleStorageWorker is the storage worker role.
	RoleStorageWorker RolesMask = 1 << 1
	// RoleKeyManager is the the key manager role.
	RoleKeyManager RolesMask = 1 << 2
	// RoleValidator is the validator role.
	RoleValidator RolesMask = 1 << 3
	// RoleConsensusRPC is the public consensus RPC services worker role.
	RoleConsensusRPC RolesMask = 1 << 4
	// RoleStorageRPC is the public storage RPC services worker role.
	RoleStorageRPC RolesMask = 1 << 5

	// RoleReserved are all the bits of the Oasis node roles bitmask
	// that are reserved and must not be used.
	RoleReserved RolesMask = ((1 << 32) - 1) & ^((RoleStorageRPC << 1) - 1)

	// Human friendly role names.
	RoleComputeWorkerName = "compute"
	RoleStorageWorkerName = "storage"
	RoleKeyManagerName    = "key-manager"
	RoleValidatorName     = "validator"
	RoleConsensusRPCName  = "consensus-rpc"
	RoleStorageRPCName    = "storage-rpc"

	rolesMaskStringSep = ","
)

// Roles returns a list of available valid roles.
func Roles() (roles []RolesMask) {
	return []RolesMask{
		RoleComputeWorker,
		RoleStorageWorker,
		RoleKeyManager,
		RoleValidator,
		RoleConsensusRPC,
		RoleStorageRPC,
	}
}

// IsSingleRole returns true if RolesMask encodes a single valid role.
func (m RolesMask) IsSingleRole() bool {
	// Ensures exactly one bit is set, and the set bit is a valid role.
	return m != 0 && m&(m-1) == 0 && m&RoleReserved == 0
}

func (m RolesMask) String() string {
	if m&RoleReserved != 0 {
		return "[invalid roles]"
	}

	var ret []string
	if m&RoleComputeWorker != 0 {
		ret = append(ret, RoleComputeWorkerName)
	}
	if m&RoleStorageWorker != 0 {
		ret = append(ret, RoleStorageWorkerName)
	}
	if m&RoleKeyManager != 0 {
		ret = append(ret, RoleKeyManagerName)
	}
	if m&RoleValidator != 0 {
		ret = append(ret, RoleValidatorName)
	}
	if m&RoleConsensusRPC != 0 {
		ret = append(ret, RoleConsensusRPCName)
	}
	if m&RoleStorageRPC != 0 {
		ret = append(ret, RoleStorageRPCName)
	}

	return strings.Join(ret, rolesMaskStringSep)
}

// MarshalText encodes a RolesMask into text form.
func (m RolesMask) MarshalText() ([]byte, error) {
	return []byte(m.String()), nil
}

func checkDuplicateRole(newRole RolesMask, curRoles RolesMask) error {
	if curRoles&newRole != 0 {
		return fmt.Errorf("%w: '%s'", ErrDuplicateRole, newRole)
	}
	return nil
}

// UnmarshalText decodes a text slice into a RolesMask.
func (m *RolesMask) UnmarshalText(text []byte) error {
	*m = 0
	roles := strings.Split(string(text), rolesMaskStringSep)
	for _, role := range roles {
		switch role {
		case RoleComputeWorkerName:
			if err := checkDuplicateRole(RoleComputeWorker, *m); err != nil {
				return err
			}
			*m |= RoleComputeWorker
		case RoleStorageWorkerName:
			if err := checkDuplicateRole(RoleStorageWorker, *m); err != nil {
				return err
			}
			*m |= RoleStorageWorker
		case RoleKeyManagerName:
			if err := checkDuplicateRole(RoleKeyManager, *m); err != nil {
				return err
			}
			*m |= RoleKeyManager
		case RoleValidatorName:
			if err := checkDuplicateRole(RoleValidator, *m); err != nil {
				return err
			}
			*m |= RoleValidator
		case RoleConsensusRPCName:
			if err := checkDuplicateRole(RoleConsensusRPC, *m); err != nil {
				return err
			}
			*m |= RoleConsensusRPC
		case RoleStorageRPCName:
			if err := checkDuplicateRole(RoleStorageRPC, *m); err != nil {
				return err
			}
			*m |= RoleStorageRPC
		default:
			return fmt.Errorf("%w: '%s'", ErrInvalidRole, role)
		}
	}
	return nil
}

// ValidateBasic performs basic descriptor validity checks.
func (n *Node) ValidateBasic(strictVersion bool) error {
	v := n.Versioned.V
	switch strictVersion {
	case true:
		// Only the latest version is allowed.
		if v != LatestNodeDescriptorVersion {
			return fmt.Errorf("invalid node descriptor version (expected: %d got: %d)",
				LatestNodeDescriptorVersion,
				v,
			)
		}
	case false:
		// A range of versions is allowed.
		if v < minNodeDescriptorVersion || v > maxNodeDescriptorVersion {
			return fmt.Errorf("invalid node descriptor version (min: %d max: %d)",
				minNodeDescriptorVersion,
				maxNodeDescriptorVersion,
			)
		}
	}
	return nil
}

// AddRoles adds a new node role to the existing roles mask.
func (n *Node) AddRoles(r RolesMask) {
	n.Roles |= r
}

// HasRoles checks if the node has the specified roles.
func (n *Node) HasRoles(r RolesMask) bool {
	return n.Roles&r != 0
}

// OnlyHasRoles checks if the node only has the specified roles and no others.
func (n *Node) OnlyHasRoles(r RolesMask) bool {
	return n.Roles == r
}

// IsExpired returns true if the node expiration epoch is strictly smaller
// than the passed (current) epoch.
func (n *Node) IsExpired(epoch uint64) bool {
	return n.Expiration < epoch
}

// GetRuntime searches for an existing supported runtime descriptor in Runtimes and returns it.
func (n *Node) GetRuntime(id common.Namespace) *Runtime {
	for _, rt := range n.Runtimes {
		if !rt.ID.Equal(&id) {
			continue
		}

		return rt
	}
	return nil
}

// AddOrUpdateRuntime searches for an existing supported runtime descriptor in Runtimes and returns
// it. In case a runtime descriptor for the given runtime doesn't exist yet, a new one is created
// appended to the list of supported runtimes and returned.
func (n *Node) AddOrUpdateRuntime(id common.Namespace) *Runtime {
	if rt := n.GetRuntime(id); rt != nil {
		return rt
	}

	rt := &Runtime{ID: id}
	n.Runtimes = append(n.Runtimes, rt)
	return rt
}

// Runtime represents the runtimes supported by a given Oasis node.
type Runtime struct {
	// ID is the public key identifying the runtime.
	ID common.Namespace `json:"id"`

	// Version is the version of the runtime.
	Version version.Version `json:"version"`

	// Capabilities are the node's capabilities for a given runtime.
	Capabilities Capabilities `json:"capabilities"`

	// ExtraInfo is the extra per node + per runtime opaque data associated
	// with the current instance.
	ExtraInfo []byte `json:"extra_info"`
}

// TLSInfo contains information for connecting to this node via TLS.
type TLSInfo struct {
	// PubKey is the public key used for establishing TLS connections.
	PubKey signature.PublicKey `json:"pub_key"`

	// NextPubKey is the public key that will be used for establishing TLS connections after
	// certificate rotation (if enabled).
	NextPubKey signature.PublicKey `json:"next_pub_key,omitempty"`

	// Addresses is the list of addresses at which the node can be reached.
	Addresses []TLSAddress `json:"addresses"`
}

// Equal compares vs another TLSInfo for equality.
func (t *TLSInfo) Equal(other *TLSInfo) bool {
	if !t.PubKey.Equal(other.PubKey) {
		return false
	}

	if !t.NextPubKey.Equal(other.NextPubKey) {
		return false
	}

	if len(t.Addresses) != len(other.Addresses) {
		return false
	}
	for i, ca := range t.Addresses {
		if !ca.Equal(&other.Addresses[i]) {
			return false
		}
	}

	return true
}

// P2PInfo contains information for connecting to this node via P2P transport.
type P2PInfo struct {
	// ID is the unique identifier of the node on the P2P transport.
	ID signature.PublicKey `json:"id"`

	// Addresses is the list of addresses at which the node can be reached.
	Addresses []Address `json:"addresses"`
}

// ConsensusInfo contains information for connecting to this node as a
// consensus member.
type ConsensusInfo struct {
	// ID is the unique identifier of the node as a consensus member.
	ID signature.PublicKey `json:"id"`

	// Addresses is the list of addresses at which the node can be reached.
	Addresses []ConsensusAddress `json:"addresses"`
}

// VRFInfo contains information for this node's participation in
// VRF based elections.
type VRFInfo struct {
	// ID is the unique identifier of the node used to generate VRF proofs.
	ID signature.PublicKey `json:"id"`
}

// Capabilities represents a node's capabilities.
type Capabilities struct {
	// TEE is the capability of a node executing batches in a TEE.
	TEE *CapabilityTEE `json:"tee,omitempty"`
}

// TEEHardware is a TEE hardware implementation.
type TEEHardware uint8

// TEE Hardware implementations.
const (
	// TEEHardwareInvalid is a non-TEE implementation.
	TEEHardwareInvalid TEEHardware = 0
	// TEEHardwareIntelSGX is an Intel SGX TEE implementation.
	TEEHardwareIntelSGX TEEHardware = 1

	// TEEHardwareReserved is the first reserved hardware implementation
	// identifier. All equal or greater identifiers are reserved.
	TEEHardwareReserved TEEHardware = TEEHardwareIntelSGX + 1

	teeInvalid  = "invalid"
	teeIntelSGX = "intel-sgx"
)

// String returns the string representation of a TEEHardware.
func (h TEEHardware) String() string {
	switch h {
	case TEEHardwareInvalid:
		return teeInvalid
	case TEEHardwareIntelSGX:
		return teeIntelSGX
	default:
		return "[unsupported TEEHardware]"
	}
}

// FromString deserializes a string into a TEEHardware.
func (h *TEEHardware) FromString(str string) error {
	switch strings.ToLower(str) {
	case "", teeInvalid:
		*h = TEEHardwareInvalid
	case teeIntelSGX:
		*h = TEEHardwareIntelSGX
	default:
		return ErrInvalidTEEHardware
	}

	return nil
}

// CapabilityTEE represents the node's TEE capability.
type CapabilityTEE struct {
	// TEE hardware type.
	Hardware TEEHardware `json:"hardware"`

	// Runtime attestation key.
	RAK signature.PublicKey `json:"rak"`

	// Attestation.
	Attestation []byte `json:"attestation"`
}

// SGXConstraints are the Intel SGX TEE constraints.
type SGXConstraints struct {
	// Enclaves is the allowed MRENCLAVE/MRSIGNER pairs.
	Enclaves []sgx.EnclaveIdentity `json:"enclaves,omitempty"`

	// AllowedQuoteStatuses are the allowed quote statuses for the node
	// to be scheduled as a compute worker.
	//
	// Note: QuoteOK is ALWAYS allowed, and does not need to be specified.
	AllowedQuoteStatuses []ias.ISVEnclaveQuoteStatus `json:"allowed_quote_statuses,omitempty"`
}

func (constraints *SGXConstraints) quoteStatusAllowed(avr *ias.AttestationVerificationReport) bool {
	status := avr.ISVEnclaveQuoteStatus

	// Always allow "OK".
	if status == ias.QuoteOK {
		return true
	}

	// Search through the constraints to see if the AVR quote status is
	// explicitly allowed.
	for _, v := range constraints.AllowedQuoteStatuses {
		if v == status {
			return true
		}
	}

	return false
}

// RAKHash computes the expected AVR report hash bound to a given public RAK.
func RAKHash(rak signature.PublicKey) hash.Hash {
	hData := make([]byte, 0, len(teeHashContext)+signature.PublicKeySize)
	hData = append(hData, teeHashContext...)
	hData = append(hData, rak[:]...)
	return hash.NewFromBytes(hData)
}

// Verify verifies the node's TEE capabilities, at the provided timestamp.
func (c *CapabilityTEE) Verify(ts time.Time, constraints []byte) error {
	rakHash := RAKHash(c.RAK)

	switch c.Hardware {
	case TEEHardwareIntelSGX:
		var avrBundle ias.AVRBundle
		if err := cbor.Unmarshal(c.Attestation, &avrBundle); err != nil {
			return err
		}

		avr, err := avrBundle.Open(ias.IntelTrustRoots, ts)
		if err != nil {
			return err
		}

		// Extract the original ISV quote.
		q, err := avr.Quote()
		if err != nil {
			return err
		}

		// Ensure that the MRENCLAVE/MRSIGNER match what is specified
		// in the TEE-specific constraints field.
		var cs SGXConstraints
		if err := cbor.Unmarshal(constraints, &cs); err != nil {
			return fmt.Errorf("node: malformed SGX constraints: %w", err)
		}
		var eidValid bool
		for _, eid := range cs.Enclaves {
			eidMrenclave := eid.MrEnclave
			eidMrsigner := eid.MrSigner
			if bytes.Equal(eidMrenclave[:], q.Report.MRENCLAVE[:]) && bytes.Equal(eidMrsigner[:], q.Report.MRSIGNER[:]) {
				eidValid = true
				break
			}
		}
		if !eidValid {
			return ErrBadEnclaveIdentity
		}

		// Ensure that the ISV quote includes the hash of the node's
		// RAK.
		var avrRAKHash hash.Hash
		_ = avrRAKHash.UnmarshalBinary(q.Report.ReportData[:hash.Size])
		if !rakHash.Equal(&avrRAKHash) {
			return ErrRAKHashMismatch
		}

		// Ensure that the quote status is acceptable.
		if !cs.quoteStatusAllowed(avr) {
			return ErrConstraintViolation
		}

		// The last 32 bytes of the quote ReportData are deliberately
		// ignored.

		return nil
	default:
		return ErrInvalidTEEHardware
	}
}

// String returns a string representation of itself.
func (n *Node) String() string {
	return "<Node id=" + n.ID.String() + ">"
}

// MultiSignedNode is a multi-signed blob containing a CBOR-serialized Node.
type MultiSignedNode struct {
	signature.MultiSigned
}

// Open first verifies the blob signatures and then unmarshals the blob.
func (s *MultiSignedNode) Open(context signature.Context, node *Node) error {
	return s.MultiSigned.Open(context, node)
}

// PrettyPrint writes a pretty-printed representation of the type
// to the given writer.
func (s MultiSignedNode) PrettyPrint(ctx context.Context, prefix string, w io.Writer) {
	pt, err := s.PrettyType()
	if err != nil {
		fmt.Fprintf(w, "%s<error: %s>\n", prefix, err)
		return
	}

	pt.(prettyprint.PrettyPrinter).PrettyPrint(ctx, prefix, w)
}

// PrettyType returns a representation of the type that can be used for pretty printing.
func (s MultiSignedNode) PrettyType() (interface{}, error) {
	var n Node
	if err := cbor.Unmarshal(s.MultiSigned.Blob, &n); err != nil {
		return nil, fmt.Errorf("malformed signed blob: %w", err)
	}
	return signature.NewPrettyMultiSigned(s.MultiSigned, n)
}

// MultiSignNode serializes the Node and multi-signs the result.
func MultiSignNode(signers []signature.Signer, context signature.Context, node *Node) (*MultiSignedNode, error) {
	multiSigned, err := signature.SignMultiSigned(signers, context, node)
	if err != nil {
		return nil, err
	}

	return &MultiSignedNode{
		MultiSigned: *multiSigned,
	}, nil
}
