package idb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/algorand/indexer/v3/types"

	sdk "github.com/algorand/go-algorand-sdk/v2/types"
)

// TxnRow is metadata relating to one transaction in a transaction query.
type TxnRow struct {
	// Round is the round where the transaction was committed.
	Round uint64

	// Round time  is the block time when the block was confirmed.
	RoundTime time.Time

	// Intra is the offset into the block where this transaction was placed.
	Intra int

	// TxnBytes is the raw signed transaction with apply data object, only used when the root txn is being returned.
	Txn *sdk.SignedTxnWithAD

	// RootTxnBytes the root transaction raw signed transaction with apply data object, only inner transactions have this.
	RootTxn *sdk.SignedTxnWithAD

	// AssetID is the ID of any asset or application created or configured by this
	// transaction.
	AssetID uint64

	// Extra are some additional fields which might be related to to the transaction.
	Extra TxnExtra

	// Error indicates that there was an internal problem processing the expected transaction.
	Error error
}

// OptionalUint wraps bool and uint. It has a custom marshaller below.
type OptionalUint struct {
	Present bool
	Value   uint
}

// MarshalText implements TextMarshaler interface.
func (ou OptionalUint) MarshalText() ([]byte, error) {
	if !ou.Present {
		return nil, nil
	}
	return []byte(fmt.Sprintf("%d", ou.Value)), nil
}

// UnmarshalText implements TextUnmarshaler interface.
func (ou *OptionalUint) UnmarshalText(text []byte) error {
	if text == nil {
		*ou = OptionalUint{}
	} else {
		value, err := strconv.ParseUint(string(text), 10, 64)
		if err != nil {
			return err
		}
		*ou = OptionalUint{
			Present: true,
			Value:   uint(value),
		}
	}

	return nil
}

// TxnExtra is some additional metadata needed for a transaction.
type TxnExtra struct {
	AssetCloseAmount uint64 `codec:"aca,omitempty"`
	// RootIntra is set only on inner transactions. Combined with the confirmation
	// round it can be used to lookup the root transaction.
	RootIntra OptionalUint `codec:"root-intra,omitempty"`
	// RootTxid is set on inner transactions. It is a convenience for the
	// future. If we decide to return inner transactions we'll want to include
	// the root txid.
	RootTxid string `codec:"root-txid,omitempty"`
}

// ErrorNotInitialized is used when requesting something that can't be returned
// because initialization has not been completed.
var ErrorNotInitialized error = errors.New("accounting not initialized")

// ErrorBlockNotFound is used when requesting a block that isn't in the DB.
var ErrorBlockNotFound = errors.New("block not found")

type IndexerDb interface {
	// Close all connections to the database. Should be called when IndexerDb is
	// no longer needed.
	Close()

	// Import a block and do the accounting.
	AddBlock(block *types.ValidatedBlock) error

	LoadGenesis(genesis sdk.Genesis) (err error)

	// GetNextRoundToAccount returns ErrorNotInitialized if genesis is not loaded.
	GetNextRoundToAccount() (uint64, error)
	GetNetworkState() (NetworkState, error)
	SetNetworkState(genesis sdk.Digest) error
	Health(ctx context.Context) (status Health, err error)
	DeleteTransactions(ctx context.Context, keep uint64) error
}

// IndexerDbOptions are the options common to all indexer backends.
type IndexerDbOptions struct {
	ReadOnly bool
	// Maximum connection number for connection pool
	// This means the total number of active queries that can be running
	// concurrently can never be more than this
	MaxConn uint32

	IndexerDatadir string
	AlgodDataDir   string
	AlgodToken     string
	AlgodAddr      string
}

// Health is the response object that IndexerDb objects need to return from the Health method.
type Health struct {
	Data        *map[string]interface{} `json:"data,omitempty"`
	Round       uint64                  `json:"round"`
	IsMigrating bool                    `json:"is-migrating"`
	DBAvailable bool                    `json:"db-available"`
	Error       string                  `json:"error"`
}

// NetworkState encodes network metastate.
type NetworkState struct {
	GenesisHash sdk.Digest `codec:"genesis-hash"`
}

// MaxTransactionsError records the error when transaction counts exceeds MaxTransactionsLimit.
type MaxTransactionsError struct {
}

func (e MaxTransactionsError) Error() string {
	return "number of transactions exceeds MaxTransactionsLimit"
}
