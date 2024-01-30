package importer

import (
	"context"
	_ "embed" // used to embed config
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/v2/encoding/json"
	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"

	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/importers"
)

const (
	// PluginName to use when configuring.
	PluginName = "ndlycloud"
)

var (
	waitForRoundTimeout = 30 * time.Second
)

type ndlyImporter struct {
	aclient       *algod.Client
	logger        *logrus.Logger
	cfg           Config
	ctx           context.Context
	cancel        context.CancelFunc
	genesis       *sdk.Genesis
	nodeLastRound uint64
}

//go:embed sample.yaml
var sampleConfig string

var algodImporterMetadata = plugins.Metadata{
	Name:         PluginName,
	Description:  "Importer for fetching blocks from an algod REST API.",
	Deprecated:   false,
	SampleConfig: sampleConfig,
}

func (algodImp *ndlyImporter) OnComplete(input data.BlockData) error {
	return nil
}

func (algodImp *ndlyImporter) Metadata() plugins.Metadata {
	return algodImporterMetadata
}

// package-wide init function
func init() {
	importers.Register(PluginName, importers.ImporterConstructorFunc(func() importers.Importer {
		return &ndlyImporter{}
	}))
}

func (algodImp *ndlyImporter) Init(ctx context.Context, initProvider data.InitProvider, cfg plugins.PluginConfig, logger *logrus.Logger) error {
	algodImp.ctx, algodImp.cancel = context.WithCancel(ctx)
	algodImp.logger = logger
	err := cfg.UnmarshalConfig(&algodImp.cfg)
	if err != nil {
		return fmt.Errorf("connect failure in unmarshalConfig: %v", err)
	}

	var client *algod.Client
	u, err := url.Parse(algodImp.cfg.NetAddr)
	if err != nil {
		return err
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		algodImp.cfg.NetAddr = "http://" + algodImp.cfg.NetAddr
		algodImp.logger.Infof("Algod Importer added http prefix to NetAddr: %s", algodImp.cfg.NetAddr)
	}
	client, err = algod.MakeClient(algodImp.cfg.NetAddr, algodImp.cfg.Token)
	if err != nil {
		return err
	}
	algodImp.aclient = client
	algodImp.nodeLastRound = 0
	genesisResponse, err := algodImp.aclient.GetGenesis().Do(algodImp.ctx)
	if err != nil {
		return err
	}

	genesis := sdk.Genesis{}

	// Don't fail on unknown properties here since the go-algorand and SDK genesis types differ slightly
	err = json.LenientDecode([]byte(genesisResponse), &genesis)
	if err != nil {
		return err
	}
	if reflect.DeepEqual(genesis, sdk.Genesis{}) {
		return fmt.Errorf("unable to fetch genesis file from API at %s", algodImp.cfg.NetAddr)
	}
	algodImp.genesis = &genesis

	return nil
}

func (algodImp *ndlyImporter) GetGenesis() (*sdk.Genesis, error) {
	if algodImp.genesis != nil {
		return algodImp.genesis, nil
	}
	return nil, fmt.Errorf("algod importer is missing its genesis: GetGenesis() should be called only after Init()")
}

func (algodImp *ndlyImporter) Close() error {
	if algodImp.cancel != nil {
		algodImp.cancel()
	}
	return nil
}

func (ndlyImp *ndlyImporter) getDelta(rnd uint64) (sdk.LedgerStateDelta, error) {
	var delta sdk.LedgerStateDelta
	params := struct {
		Format string `url:"format,omitempty"`
	}{Format: "msgp"}
	// Note: this uses lenient decoding. GetRaw and msgpack.Decode would allow strict decoding.
	err := (*common.Client)(ndlyImp.aclient).GetRawMsgpack(ndlyImp.ctx, &delta, fmt.Sprintf("/v2/deltas/%d", rnd), params, nil)
	ndlyImp.logger.Tracef("importer algod.getDelta() called /v2/deltas/%d err: %v", rnd, err)
	if err != nil {
		return sdk.LedgerStateDelta{}, err
	}

	return delta, nil
}

// SyncError is used to indicate algod and conduit are not synchronized.
type SyncError struct {
	// retrievedRound is the round returned from an algod status call.
	retrievedRound uint64

	// expectedRound is the round conduit expected to have gotten back.
	expectedRound uint64

	// err is the error that was received from the endpoint caller.
	err error
}

// NewSyncError creates a new SyncError.
func NewSyncError(retrievedRound, expectedRound uint64, err error) *SyncError {
	return &SyncError{
		retrievedRound: retrievedRound,
		expectedRound:  expectedRound,
		err:            err,
	}
}

func (e *SyncError) Error() string {
	return fmt.Sprintf("wrong round returned from status for round: retrieved(%d) != expected(%d): %v", e.retrievedRound, e.expectedRound, e.err)
}

func (e *SyncError) Unwrap() error {
	return e.err
}

func waitForRoundWithTimeout(ctx context.Context, l *logrus.Logger, c *algod.Client, rnd uint64, to time.Duration) (uint64, error) {
	ctxWithTimeout, cf := context.WithTimeout(ctx, to)
	defer cf()
	status, err := c.StatusAfterBlock(rnd - 1).Do(ctxWithTimeout)
	l.Tracef("importer algod.waitForRoundWithTimeout() called StatusAfterBlock(%d) err: %v", rnd-1, err)

	if err == nil {
		// When c.StatusAfterBlock has a server-side timeout it returns the current status.
		// We use a context with timeout and the algod default timeout is 1 minute, so technically
		// with the current versions, this check should never be required.
		if rnd <= status.LastRound {
			return status.LastRound, nil
		}
		// algod's timeout should not be reached because context.WithTimeout is used
		return 0, NewSyncError(status.LastRound, rnd, fmt.Errorf("sync error, likely due to status after block timeout"))
	}

	// If there was a different error and the node is responsive, call status before returning a SyncError.
	status2, err2 := c.Status().Do(ctx)
	l.Tracef("importer algod.waitForRoundWithTimeout() called Status() err: %v", err2)
	if err2 != nil {
		// If there was an error getting status, return the original error.
		return 0, fmt.Errorf("unable to get status after block and status: %w", errors.Join(err, err2))
	}
	if status2.LastRound < rnd {
		return 0, NewSyncError(status2.LastRound, rnd, fmt.Errorf("status2.LastRound mismatch: %w", err))
	}

	// This is probably a connection error, not a SyncError.
	return 0, fmt.Errorf("unknown errors: StatusAfterBlock(%w), Status(%w)", err, err2)
}

func (algodImp *ndlyImporter) getBlockInner(rnd uint64) (data.BlockData, error) {
	var blockbytes []byte
	var blk data.BlockData

	//skip WFRs up to the last round @ node
	if rnd > algodImp.nodeLastRound {
		nodeRound, err := waitForRoundWithTimeout(algodImp.ctx, algodImp.logger, algodImp.aclient, rnd, waitForRoundTimeout)
		if err != nil {
			err = fmt.Errorf("called waitForRoundWithTimeout: %w", err)
			algodImp.logger.Errorf(err.Error())
			return data.BlockData{}, err
		}
		algodImp.nodeLastRound = nodeRound
	}

	var errDelta error = nil
	var wg sync.WaitGroup
	wg.Add(1)

	//Get Delta and Block in parallel
	go func() {
		// Round 0 has no delta associated with it
		if rnd != 0 {
			var delta sdk.LedgerStateDelta
			delta, errDelta = algodImp.getDelta(rnd)
			if errDelta == nil {
				blk.Delta = &delta
			}
		}
		wg.Done()
	}()

	start := time.Now()
	blockbytes, err := algodImp.aclient.BlockRaw(rnd).Do(algodImp.ctx)
	algodImp.logger.Tracef("importer algod.GetBlock() called BlockRaw(%d) err: %v", rnd, err)
	dt := time.Since(start)
	getAlgodRawBlockTimeSeconds.Observe(dt.Seconds())

	//wait for Delta
	wg.Wait()

	if err != nil {
		err = fmt.Errorf("error getting block for round %d: %w", rnd, err)
		algodImp.logger.Errorf(err.Error())
		algodImp.nodeLastRound = 0
		return data.BlockData{}, err
	}
	tmpBlk := new(models.BlockResponse)
	err = msgpack.Decode(blockbytes, tmpBlk)
	if err != nil {
		algodImp.nodeLastRound = 0
		return blk, fmt.Errorf("error decoding block for round %d: %w", rnd, err)
	}

	blk.BlockHeader = tmpBlk.Block.BlockHeader
	blk.Payset = tmpBlk.Block.Payset
	blk.Certificate = tmpBlk.Cert

	if errDelta != nil {
		if algodImp.nodeLastRound < rnd {
			err = fmt.Errorf("ledger state delta not found: node round (%d) is behind required round (%d), ensure follower node has its sync round set to the required round: %w", algodImp.nodeLastRound, rnd, err)
		} else {
			err = fmt.Errorf("ledger state delta not found: node round (%d), required round (%d): verify follower node configuration and ensure follower node has its sync round set to the required round, re-deploying the follower node may be necessary: %w", algodImp.nodeLastRound, rnd, err)
		}
		algodImp.nodeLastRound = 0
		algodImp.logger.Error(err.Error())
		return data.BlockData{}, err
	}

	return blk, err
}

func (algodImp *ndlyImporter) GetBlock(rnd uint64) (data.BlockData, error) {
	blk, err := algodImp.getBlockInner(rnd)

	if err != nil {
		target := &SyncError{}
		if errors.As(err, &target) {
			algodImp.logger.Warnf("importer algod.GetBlock() sync error detected, attempting to set the sync round to recover the node: %s", err.Error())
			_, _ = algodImp.aclient.SetSyncRound(rnd).Do(algodImp.ctx)
		} else {
			err = fmt.Errorf("importer algod.GetBlock() error getting block for round %d, check node configuration: %s", rnd, err)
			algodImp.logger.Errorf(err.Error())
		}
		return data.BlockData{}, err
	}

	return blk, nil

}

func (algodImp *ndlyImporter) ProvideMetrics(subsystem string) []prometheus.Collector {
	getAlgodRawBlockTimeSeconds = initGetAlgodRawBlockTimeSeconds(subsystem)
	return []prometheus.Collector{
		getAlgodRawBlockTimeSeconds,
	}
}
