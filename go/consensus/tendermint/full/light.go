package full

import (
	"context"
	"fmt"

	tmtypes "github.com/tendermint/tendermint/types"

	"github.com/oasisprotocol/oasis-core/go/common/cbor"
	consensusAPI "github.com/oasisprotocol/oasis-core/go/consensus/api"
	"github.com/oasisprotocol/oasis-core/go/consensus/api/transaction"
	coreState "github.com/oasisprotocol/oasis-core/go/consensus/tendermint/abci/state"
	"github.com/oasisprotocol/oasis-core/go/storage/mkvs/syncer"
)

// Implements LightClientBackend.
func (t *fullService) GetLightBlock(ctx context.Context, height int64) (*consensusAPI.LightBlock, error) {
	if err := t.ensureStarted(ctx); err != nil {
		return nil, err
	}

	tmHeight, err := t.heightToTendermintHeight(height)
	if err != nil {
		return nil, err
	}

	var lb tmtypes.LightBlock

	// Don't use the client as that imposes stupid pagination. Access the state database directly.
	lb.ValidatorSet, err = t.stateStore.LoadValidators(tmHeight)
	if err != nil {
		return nil, consensusAPI.ErrVersionNotFound
	}

	if commit, cerr := t.client.Commit(ctx, &tmHeight); cerr == nil && commit.Header != nil {
		lb.SignedHeader = &commit.SignedHeader
		tmHeight = commit.Header.Height
	}
	protoLb, err := lb.ToProto()
	if err != nil {
		return nil, fmt.Errorf("tendermint: failed to convert light block: %w", err)
	}
	if protoLb.ValidatorSet != nil {
		// ToProto sets the TotalVotingPower to 0, but the rust side FromProto requires it.
		// https://github.com/tendermint/tendermint/blob/41c176ccc6a75d25631d0f891efb2e19a33329dc/types/validator_set.go#L949-L951
		// https://github.com/informalsystems/tendermint-rs/blob/c70f6eea9ccd1f41c0a608c5285b6af98b66c9fe/tendermint/src/validator.rs#L38-L45
		protoLb.ValidatorSet.TotalVotingPower = lb.ValidatorSet.TotalVotingPower()
	}

	meta, err := protoLb.Marshal()
	if err != nil {
		return nil, fmt.Errorf("tendermint: failed to marshal light block: %w", err)
	}

	return &consensusAPI.LightBlock{
		Height: tmHeight,
		Meta:   meta,
	}, nil
}

// Implements LightClientBackend.
func (t *fullService) GetParameters(ctx context.Context, height int64) (*consensusAPI.Parameters, error) {
	if err := t.ensureStarted(ctx); err != nil {
		return nil, err
	}

	tmHeight, err := t.heightToTendermintHeight(height)
	if err != nil {
		return nil, err
	}
	params, err := t.client.ConsensusParams(ctx, &tmHeight)
	if err != nil {
		return nil, fmt.Errorf("%w: tendermint: consensus params query failed: %s", consensusAPI.ErrVersionNotFound, err.Error())
	}
	meta, err := params.ConsensusParams.Marshal()
	if err != nil {
		return nil, fmt.Errorf("tendermint: failed to marshal consensus params: %w", err)
	}

	cs, err := coreState.NewImmutableState(ctx, t.mux.State(), height)
	if err != nil {
		return nil, fmt.Errorf("tendermint: failed to initialize core consensus state: %w", err)
	}
	cp, err := cs.ConsensusParameters(ctx)
	if err != nil {
		return nil, fmt.Errorf("tendermint: failed to fetch core consensus parameters: %w", err)
	}

	return &consensusAPI.Parameters{
		Height:     params.BlockHeight,
		Parameters: *cp,
		Meta:       meta,
	}, nil
}

// Implements LightClientBackend.
func (t *fullService) State() syncer.ReadSyncer {
	return t.mux.State().Storage()
}

// Implements LightClientBackend.
func (t *fullService) SubmitTxNoWait(ctx context.Context, tx *transaction.SignedTransaction) error {
	return t.broadcastTxRaw(cbor.Marshal(tx))
}
