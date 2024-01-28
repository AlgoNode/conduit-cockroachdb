package exporter

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"

	"github.com/algonode/conduit-cockroachdb/plugin/exporter/idb"
	_ "github.com/algonode/conduit-cockroachdb/plugin/exporter/idb/cockroach"
	"github.com/algonode/conduit-cockroachdb/plugin/exporter/idb/cockroach/util"

	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/exporters"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"
	"github.com/algorand/indexer/v3/types"
)

//go:embed sample.yaml
var sampleConfig string
var errMissingDelta = errors.New("ledger state delta is missing from block, ensure algod importer is using 'follower' mode")

// metadata contains information about the plugin used for CLI helpers.
var metadata = plugins.Metadata{
	Name:         "cockroachdb",
	Description:  "CockroachDB exporter.",
	Deprecated:   false,
	SampleConfig: sampleConfig,
}

func init() {
	exporters.Register(metadata.Name, exporters.ExporterConstructorFunc(func() exporters.Exporter {
		return &cockroachdbExporter{}
	}))
}

type ExporterConfig struct {
	ConnectionString string                      `yaml:"connection-string"`
	Test             bool                        `yaml:"test"`
	MaxConn          uint32                      `yaml:"max-conn"`
	Delete           util.CDBPruneConfigurations `yaml:"delete-task"`
}

// ExporterTemplate is the object which implements the exporter plugin interface.
type cockroachdbExporter struct {
	cfg    ExporterConfig
	ctx    context.Context
	cf     context.CancelFunc
	logger *logrus.Logger
	db     idb.IndexerDb
	round  uint64
	wg     sync.WaitGroup
	dm     util.CDBDataManager
}

func (exp *cockroachdbExporter) Metadata() plugins.Metadata {
	return metadata
}

func (exp *cockroachdbExporter) Config() string {
	ret, _ := yaml.Marshal(exp.cfg)
	return string(ret)
}

func (exp *cockroachdbExporter) Close() error {
	if exp.db != nil {
		exp.db.Close()
	}
	return nil
}

// createIndexerDB common code for creating the IndexerDb instance.
func createIndexerDB(logger *logrus.Logger, readonly bool, cfg plugins.PluginConfig) (idb.IndexerDb, chan struct{}, ExporterConfig, error) {
	var eCfg ExporterConfig
	if err := cfg.UnmarshalConfig(&eCfg); err != nil {
		return nil, nil, eCfg, fmt.Errorf("connect failure in unmarshalConfig: %v", err)
	}

	logger.Debugf("createIndexerDB: eCfg.Delete=%+v", eCfg.Delete)

	// Inject a dummy db for unit testing
	dbName := "cockroachdb"
	if eCfg.Test {
		dbName = "dummy"
	}

	// for some reason when ConnectionString is empty, it's automatically
	// connecting to a local instance that's running.
	// this behavior can be reproduced in TestConnectDbFailure.
	if !eCfg.Test && eCfg.ConnectionString == "" {
		return nil, nil, eCfg, fmt.Errorf("connection string is empty for %s", dbName)
	}

	var opts idb.IndexerDbOptions
	opts.ReadOnly = readonly
	opts.MaxConn = eCfg.MaxConn

	db, ready, err := idb.IndexerDbByName(dbName, eCfg.ConnectionString, opts, logger)
	if err != nil {
		return nil, nil, eCfg, fmt.Errorf("connect failure constructing db, %s: %v", dbName, err)
	}

	return db, ready, eCfg, nil
}

func (exp *cockroachdbExporter) Init(ctx context.Context, ip data.InitProvider, cfg plugins.PluginConfig, logger *logrus.Logger) error {
	exp.ctx, exp.cf = context.WithCancel(ctx)
	exp.logger = logger

	db, ready, exporterConfig, err := createIndexerDB(exp.logger, false, cfg)
	if err != nil {
		return fmt.Errorf("db create error: %v", err)
	}
	exp.cfg = exporterConfig
	<-ready

	exp.db = db
	_, err = idb.EnsureInitialImport(exp.db, *ip.GetGenesis())
	if err != nil {
		return fmt.Errorf("error importing genesis: %v", err)
	}
	dbRound, err := db.GetNextRoundToAccount()
	if err != nil {
		return fmt.Errorf("error getting next db round : %v", err)
	}
	if uint64(ip.NextDBRound()) != dbRound {
		return fmt.Errorf("initializing block round %d but next round to account is %d", ip.NextDBRound(), dbRound)
	}
	exp.round = uint64(ip.NextDBRound())

	dataPruningEnabled := !exp.cfg.Test && exp.cfg.Delete.Rounds > 0
	exp.logger.Debugf("cockroachdb exporter Init(): data pruning enabled: %t; exp.cfg.Delete: %+v", dataPruningEnabled, exp.cfg.Delete)
	if dataPruningEnabled {
		exp.dm = util.MakeDataManager(exp.ctx, &exp.cfg.Delete, exp.db, logger)
		exp.wg.Add(1)
		go exp.dm.DeleteLoop(&exp.wg, &exp.round)
	}

	return nil

}

func (exp *cockroachdbExporter) Receive(exportData data.BlockData) error {
	if exportData.Delta == nil {
		if exportData.Round() == 0 {
			exportData.Delta = &sdk.LedgerStateDelta{}
		} else {
			return errMissingDelta
		}
	}
	vb := types.ValidatedBlock{
		Block: sdk.Block{BlockHeader: exportData.BlockHeader, Payset: exportData.Payset},
		Delta: *exportData.Delta,
	}
	if err := exp.db.AddBlock(&vb); err != nil {
		return err
	}
	exp.round = exportData.Round() + 1
	return nil
}

// RoundRequest connects to the database, queries the round, and closes the
// connection. If there is a problem with the configuration, an error will be
// returned in the Init phase and this function will return 0.
func (exp *cockroachdbExporter) RoundRequest(cfg plugins.PluginConfig) (uint64, error) {
	nullLogger := logrus.New()
	nullLogger.Out = io.Discard // no logging

	db, _, _, err := createIndexerDB(nullLogger, true, cfg)
	if err != nil {
		// Assume the error is related to an uninitialized DB.
		// If it is something more serious, the failure will be detected during Init.
		return 0, nil
	}

	rnd, err := db.GetNextRoundToAccount()
	// ignore non initialized error because that would happen during Init.
	// This case probably wont be hit except in very unusual cases because
	// in most cases `createIndexerDB` would fail first.
	if err != nil && err != idb.ErrorNotInitialized {
		return 0, fmt.Errorf("postgres.RoundRequest(): failed to get next round: %w", err)
	}

	return rnd, nil
}
