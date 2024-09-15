package importer

import (
	"context"
	_ "embed"
	"fmt"
	"reflect"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/v2/encoding/json"
	"github.com/algorand/go-algorand-sdk/v2/types"
	"github.com/labstack/gommon/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/importers"
)

const defaultNumWorkers = 8

//go:embed sample.yaml
var sampleConfig string

// metadata contains information about the plugin used for CLI helpers.
var metadata = plugins.Metadata{
	Name:         "ndlycloud",
	Description:  "Importer that fetches blocks concurrently from an algod v2 REST API.",
	Deprecated:   false,
	SampleConfig: sampleConfig,
}

func init() {
	importers.Register(metadata.Name, importers.ImporterConstructorFunc(func() importers.Importer {
		return &importerPlugin{}
	}))
}

type Config struct {
	NetAddr string `yaml:"netaddr"`
	Token   string `yaml:"token"`
	Workers uint64 `yaml:"workers"`
}

// importerPlugin is the object which implements the `importers.Importer` interface.
type importerPlugin struct {
	log    *logrus.Logger
	cfg    Config
	wp     *workerPool
	client *algod.Client
	ctx    context.Context
}

func (it *importerPlugin) Metadata() plugins.Metadata {
	return metadata
}

func (it *importerPlugin) Close() error {
	it.wp.close()
	return nil
}

func (it *importerPlugin) Init(ctx context.Context, initProvider data.InitProvider, cfg plugins.PluginConfig, logger *logrus.Logger) error {

	log.Infof("Nodely importer - initializing from round %d", initProvider.NextDBRound())

	it.log = logger
	it.ctx = ctx

	// parse configuration
	var err error
	if err = cfg.UnmarshalConfig(&it.cfg); err != nil {
		return fmt.Errorf("unable to read configuration: %w", err)
	}
	if it.cfg.Workers < 1 {
		it.cfg.Workers = defaultNumWorkers
		logger.Infof("setting number of workers to a default value of %d", it.cfg.Workers)
	}
	logger.Infof("CONFIG netaddr=%s workers=%d", it.cfg.NetAddr, it.cfg.Workers)

	// initialize the algod v2 client
	it.client, err = algod.MakeClient(it.cfg.NetAddr, it.cfg.Token)
	if err != nil {
		return fmt.Errorf("failed to initialize algod client: %w", err)
	}

	// initialize worker pool
	it.wp, err = newWorkerPool(ctx, logger, it.client, it.cfg.Workers, uint64(initProvider.NextDBRound()))
	if err != nil {
		return fmt.Errorf("failed to initialize worker pool: %w", err)
	}

	return nil
}

func (it *importerPlugin) GetGenesis() (*types.Genesis, error) {

	genesisResponse, err := it.client.GetGenesis().Do(it.ctx)
	if err != nil {
		return nil, err
	}

	genesis := types.Genesis{}

	// Don't fail on unknown properties here since the go-algorand and SDK genesis types differ slightly
	err = json.LenientDecode([]byte(genesisResponse), &genesis)
	if err != nil {
		return nil, err
	}
	if reflect.DeepEqual(genesis, types.Genesis{}) {
		return nil, fmt.Errorf("unable to fetch genesis file from Algod v2 API at %s", it.cfg.NetAddr)
	}
	return &genesis, nil
}

func (it *importerPlugin) GetBlock(rnd uint64) (data.BlockData, error) {

	log.Infof("Nodely importer - GetBlock(%d)", rnd)

	blk := it.wp.getItem(rnd)
	return *blk, nil
}

func (algodImp *importerPlugin) ProvideMetrics(subsystem string) []prometheus.Collector {
	getAlgodRawBlockTimeSeconds = initGetAlgodRawBlockTimeSeconds(subsystem)
	return []prometheus.Collector{
		getAlgodRawBlockTimeSeconds,
	}
}
