package util

import (
	"context"
	"sync"
	"time"

	"github.com/algonode/conduit-cockroachdb/plugin/exporter/idb"
	"github.com/sirupsen/logrus"
)

// Interval determines how often to delete data
type Interval int

const (
	once     Interval = -1
	disabled Interval = 0
	d                 = 2 * time.Second
)

// PruneConfigurations contains the configurations for data pruning
type CDBPruneConfigurations struct {
	// Rounds to keep, a value of zero results in no data pruning
	Rounds uint64 `yaml:"rounds"`
	// Interval used to prune the data. The values can be -1 to run at startup,
	// 0 to disable or N to run every N rounds.
	Interval Interval `yaml:"interval"`
}

// CDBDataManager is a data pruning interface
type CDBDataManager interface {
	DeleteLoop(*sync.WaitGroup, *uint64)
}

type cdb struct {
	config   *CDBPruneConfigurations
	db       idb.IndexerDb
	logger   *logrus.Logger
	ctx      context.Context
	duration time.Duration
}

// MakeDataManager initializes resources need for removing data from data source
func MakeDataManager(ctx context.Context, cfg *CDBPruneConfigurations, db idb.IndexerDb, logger *logrus.Logger) CDBDataManager {

	dm := &cdb{
		config:   cfg,
		db:       db,
		logger:   logger,
		ctx:      ctx,
		duration: d,
	}

	return dm
}

// DeleteLoop removes data from the txn table in Postgres DB
func (p *cdb) DeleteLoop(wg *sync.WaitGroup, nextRound *uint64) {

	defer wg.Done()

	p.logger.Debugf("DeleteLoop(): starting delete loop")
	// If the interval is disabled
	if p.config.Interval == disabled {
		// A helpful warning to say that despite a number of rounds being above 0
		// data pruning isn't going to occur
		if p.config.Rounds > 0 {
			p.logger.Warnf("DeleteLoop(): Round value was above 0 (%d) but interval was disabled. No data pruning will occur.", p.config.Rounds)
		}
		return
	}

	// round value used for interval calculation
	round := *nextRound
	func() {
		for {
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(p.duration):
				currentRound := *nextRound
				// keep, remove data older than keep
				keep := currentRound - p.config.Rounds
				if p.config.Interval == once {
					if currentRound > p.config.Rounds {
						err := p.db.DeleteTransactions(p.ctx, keep)
						if err != nil {
							p.logger.Warnf("DeleteLoop(): data pruning err: %v", err)
						}
					}
					return
				} else if p.config.Interval > disabled {
					// *nextRound should increment as exporter receives new block
					if currentRound > p.config.Rounds && currentRound-round >= uint64(p.config.Interval) {
						err := p.db.DeleteTransactions(p.ctx, keep)
						if err != nil {
							p.logger.Warnf("DeleteLoop(): data pruning err: %v", err)
						} else {
							// update round value for next interval calculation
							round = currentRound
						}
					}
				} else {
					p.logger.Fatalf("DeleteLoop(): unsupported interval value %v", p.config.Interval)
					return
				}
			}
		}
	}()
	p.logger.Warn("DeleteLoop(): loop terminated")
}
