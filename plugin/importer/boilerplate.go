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
		return &importerTemplate{}
	}))
}

type Config struct {
	// TODO: your configuration here.
	ConfigString string `yaml:"config_string"`
	ConfigInt    int    `yaml:"config_int"`
}

// importerTemplate is the object which implements the importer plugin interface.
type importerTemplate struct {
	log *logrus.Logger
	cfg Config
	wp  *workerPool
}

func (it *importerTemplate) Metadata() plugins.Metadata {
	return metadata
}

func (it *importerTemplate) Config() string {
	ret, _ := yaml.Marshal(it.cfg)
	return string(ret)
}

func (it *importerTemplate) Close() error {
	return nil
}

func (it *importerTemplate) Init(ctx context.Context, initProvider data.InitProvider, cfg plugins.PluginConfig, logger *logrus.Logger) error {
	it.log = logger
	var err error
	if err = cfg.UnmarshalConfig(&it.cfg); err != nil {
		return fmt.Errorf("unable to read configuration: %w", err)
	}
	it.wp, err = newWorkerPool(ctx, logger, 3, initProvider.NextDBRound()) //FIXME hardcoded value
	if err != nil {
		return fmt.Errorf("failed to initialize worker pool: %w", err)
	}

	// TODO: Your init logic here.
	log.Info("INIT() CALLED", initProvider.NextDBRound())

	return nil
}

func (it *importerTemplate) GetGenesis() (*types.Genesis, error) {
	return &types.Genesis{}, nil
}

func (it *importerTemplate) GetBlock(rnd uint64) (data.BlockData, error) {

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
