package importer

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/algorand/go-algorand-sdk/v2/types"
	"github.com/labstack/gommon/log"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"

	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/importers"
)

////go:embed sample.yaml
//var sampleConfig string

// metadata contains information about the plugin used for CLI helpers.
var metadata = plugins.Metadata{
	Name:        "ndlycloud",
	Description: "Example importer.",
	Deprecated:  false,
	//SampleConfig: sampleConfig,
}

func init() {
	importers.Register(metadata.Name, importers.ImporterConstructorFunc(func() importers.Importer {
		return &importerPlugin{}
	}))
}

type Config struct {
	// TODO: your configuration here.
	ConfigString string `yaml:"config_string"`
	ConfigInt    int    `yaml:"config_int"`
}

// importerPlugin is the object which implements the importerPlugin plugin interface.
type importerPlugin struct {
	log *logrus.Logger
	cfg Config
	wp  *workerPool
}

func (it *importerPlugin) Metadata() plugins.Metadata {
	return metadata
}

func (it *importerPlugin) Config() string {
	ret, _ := yaml.Marshal(it.cfg)
	return string(ret)
}

func (it *importerPlugin) Close() error {
	it.wp.close()
	return nil
}

func (it *importerPlugin) Init(ctx context.Context, initProvider data.InitProvider, cfg plugins.PluginConfig, logger *logrus.Logger) error {
	it.log = logger
	var err error
	if err = cfg.UnmarshalConfig(&it.cfg); err != nil {
		return fmt.Errorf("unable to read configuration: %w", err)
	}
	it.wp, err = newWorkerPool(ctx, logger, 3, uint64(initProvider.NextDBRound())) //FIXME hardcoded value
	if err != nil {
		return fmt.Errorf("failed to initialize worker pool: %w", err)
	}

	// TODO: Your init logic here.
	log.Info("INIT() CALLED", initProvider.NextDBRound())

	return nil
}

func (it *importerPlugin) GetGenesis() (*types.Genesis, error) {
	return &types.Genesis{}, nil
}

func (it *importerPlugin) GetBlock(rnd uint64) (data.BlockData, error) {

	// TODO: Your receive block data logic here.
	log.Info("GETBLOCK() CALLED", rnd)

	r := it.wp.getItem(rnd)

	data := data.BlockData{
		BlockHeader: types.BlockHeader{Round: r},
		Delta:       nil,
		Payset:      nil,
		Certificate: nil,
	}
	return data, nil
}
