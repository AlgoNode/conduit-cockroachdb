package dummy

import (
	"context"

	log "github.com/sirupsen/logrus"

	"github.com/algonode/conduit-cockroachdb/plugin/exporter/idb"
	"github.com/algorand/indexer/v3/types"

	sdk "github.com/algorand/go-algorand-sdk/v2/types"
)

type dummyIndexerDb struct {
	log *log.Logger
}

// IndexerDb is a mock implementation of IndexerDb
func IndexerDb() idb.IndexerDb {
	return &dummyIndexerDb{}
}

func (db *dummyIndexerDb) Close() {
}

func (db *dummyIndexerDb) AddBlock(block *types.ValidatedBlock) error {
	db.log.Printf("AddBlock")
	return nil
}

// LoadGenesis is part of idb.IndexerDB
func (db *dummyIndexerDb) LoadGenesis(genesis sdk.Genesis) (err error) {
	return nil
}

// GetNextRoundToAccount is part of idb.IndexerDB
func (db *dummyIndexerDb) GetNextRoundToAccount() (uint64, error) {
	return 0, nil
}

// GetNextRoundToLoad is part of idb.IndexerDB
func (db *dummyIndexerDb) GetNextRoundToLoad() (uint64, error) {
	return 0, nil
}

// GetSpecialAccounts is part of idb.IndexerDb
func (db *dummyIndexerDb) GetSpecialAccounts(ctx context.Context) (types.SpecialAddresses, error) {
	return types.SpecialAddresses{}, nil
}

// Health is part of idb.IndexerDB
func (db *dummyIndexerDb) Health(ctx context.Context) (state idb.Health, err error) {
	return idb.Health{}, nil
}

// GetNetworkState is part of idb.IndexerDB
func (db *dummyIndexerDb) GetNetworkState() (state idb.NetworkState, err error) {
	return idb.NetworkState{GenesisHash: sdk.Genesis{}.Hash()}, nil
}

// SetNetworkState is part of idb.IndexerDB
func (db *dummyIndexerDb) SetNetworkState(genesis sdk.Digest) error {
	return nil
}

// SetNetworkState is part of idb.IndexerDB
func (db *dummyIndexerDb) DeleteTransactions(ctx context.Context, keep uint64) error {
	return nil
}
