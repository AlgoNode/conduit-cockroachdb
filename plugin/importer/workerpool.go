package importer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	"github.com/algorand/go-algorand-sdk/v2/types"
	"github.com/sirupsen/logrus"
)

const AlgodRetryTimeout = 5 * time.Second

type workerPool struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	logger *logrus.Logger

	client *algod.Client

	jobsCh chan<- types.Round

	roundsCh <-chan *data.BlockData
	rounds   sortedList

	numWorkers uint64

	// shared state
	mutex      sync.Mutex
	windowLow  uint64
	windowHigh uint64
	lastRound  uint64
}

// the worker pool assumes that the caller will fetch the blocks in order, starting from `initialRound`
func newWorkerPool(
	parentCtx context.Context,
	logger *logrus.Logger,
	numWorkers uint64,
	initialRound uint64,
) (*workerPool, error) {

	ctx, cancelFunc := context.WithCancel(parentCtx)

	// initialize the algod v2 client
	client, err := algod.MakeClient("https://mainnet-api.algonode.cloud", "")
	if err != nil {
		cancelFunc()
		return nil, fmt.Errorf("failed to initialize algod: %w", err)
	}

	// query algod for the current round
	status, err := client.Status().Do(ctx)
	if err != nil {
		cancelFunc()
		return nil, fmt.Errorf("failed to query current round: %w", err)
	}
	logger.Tracef("the last known round is %d", status.LastRound)

	jobsCh := make(chan types.Round, numWorkers)
	roundsCh := make(chan *data.BlockData, numWorkers)

	// spawn workers
	for i := uint64(0); i < numWorkers; i++ {
		go workerEntrypoint(ctx, logger, client, jobsCh, roundsCh)
	}

	// this struct has some shared state with the tip follower goroutine
	wp := workerPool{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		client:     client,
		logger:     logger,
		jobsCh:     jobsCh,
		roundsCh:   roundsCh,
		numWorkers: numWorkers,

		// invariant: windowLog points to the first round in the sliding window.
		windowLow: initialRound,
		// invariant: windowHigh points one round above the sliding window.
		// Upon initialization, the sliding window has size=0, which results in windowLow=windowHigh.
		windowHigh: initialRound,
		lastRound:  status.LastRound,
	}

	go tipFollowerEntrypoint(ctx, logger, client, &wp)

	return &wp, nil
}

func (wp *workerPool) close() {
	close(wp.jobsCh)
	wp.cancelFunc()
}

func (wp *workerPool) getItem(rnd uint64) *data.BlockData {

	// abort if conduit requests rounds out of order
	{
		wp.mutex.Lock()
		if rnd != uint64(wp.windowLow) {
			wp.logger.Errorf("rnd != wp.windowLow (%d != %d)", rnd, wp.windowLow)
			panic("")
		}
		wp.mutex.Unlock()
	}

	for {
		// check if we already have the round
		{
			wp.mutex.Lock()
			if wp.rounds.Len() != 0 && wp.rounds.Min() == uint64(wp.windowLow) {

				// take out the item
				tmp := wp.rounds.PopMin()

				// update the sliding window size
				wp.windowLow++

				// create new jobs if needed (hence updating the window size)
				wp.advanceWindow()

				wp.mutex.Unlock()
				return tmp
			}
			wp.mutex.Unlock()
		}

		// wait for more data
		item := <-wp.roundsCh
		wp.rounds.Push(item)
	}

}

func (wp *workerPool) advanceWindow() {

	wp.logger.Tracef("checking whether to advance the sliding window windowLow=%d windowHigh=%d windowSize=%d lastRound=%d numWorkers=%d",
		wp.windowLow, wp.windowHigh, wp.windowHigh-wp.windowLow, wp.lastRound, wp.numWorkers)

	for {
		if (wp.windowHigh <= wp.lastRound) && // Make sure the sliding window doesn't go past the chain tip
			(wp.windowHigh-wp.windowLow) < wp.numWorkers { // If the sliding window is smaller than NUM_WORKERS, we can advance it

			wp.jobsCh <- types.Round(wp.windowHigh)
			wp.windowHigh++
		} else {
			break
		}
	}
}

func workerEntrypoint(
	ctx context.Context,
	logger *logrus.Logger,
	client *algod.Client,
	jobsCh <-chan types.Round,
	roundsCh chan<- *data.BlockData,
) {

	for {
		round := <-jobsCh

		// Fetch the block and delta from the network
		var wg sync.WaitGroup

		// fetch the block concurrently
		var block models.BlockResponse
		wg.Add(1)
		go func() {
			for {
				// download the block
				start := time.Now()
				blockbytes, err := client.BlockRaw(uint64(round)).Do(ctx)
				if err != nil {
					logger.Warnf("error calling /v2/blocks/%d: %v", round, err)
					time.Sleep(AlgodRetryTimeout)
					continue
				}
				dt := time.Since(start)

				// decode the block msgpack
				err = msgpack.Decode(blockbytes, &block)
				if err != nil {
					logger.Warnf("failed to decode algod block response for round %d: %v", round, err)
					time.Sleep(AlgodRetryTimeout)
					continue
				}

				// update metrics
				getAlgodRawBlockTimeSeconds.Observe(dt.Seconds())
				break
			}

			wg.Done()
		}()

		// fetch the delta concurrently
		var delta types.LedgerStateDelta
		wg.Add(1)
		go func() {

			params := struct {
				Format string `url:"format,omitempty"`
			}{Format: "msgp"}

			for {
				// Note: this uses lenient decoding. GetRaw and msgpack.Decode would allow strict decoding.
				err := (*common.Client)(client).GetRawMsgpack(ctx, &delta, fmt.Sprintf("/v2/deltas/%d", round), params, nil)
				if err != nil {
					logger.Warnf("failed to get /v2/deltas/%d: %v", round, err)
					time.Sleep(AlgodRetryTimeout)
					continue
				}
				break
			}

			wg.Done()
		}()

		// wait for goroutines to finish, then collect the data
		wg.Wait()
		blk := data.BlockData{
			BlockHeader: block.Block.BlockHeader,
			Payset:      block.Block.Payset,
			Certificate: block.Cert,
			Delta:       &delta,
		}

		roundsCh <- &blk
	}
}

func tipFollowerEntrypoint(
	ctx context.Context,
	logger *logrus.Logger,
	client *algod.Client,
	wp *workerPool,
) {

	for {
		// advance the sliding window if needed
		{
			wp.mutex.Lock()
			wp.advanceWindow()
			wp.mutex.Unlock()
		}

		// query the blockchain's last known round
		var lastRound uint64
		{
			wp.mutex.Lock()
			lastRound = wp.lastRound
			wp.mutex.Unlock()
		}

		// Wait for the next round
		status, err := client.StatusAfterBlock(lastRound).Do(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				logger.Trace("tip follower leaving: context cancelled")
				return
			}
			logger.Errorf("failed to get status from algod: %s", err)
			time.Sleep(AlgodRetryTimeout)
			continue
		}

		// set the blockchain's last known round
		{
			wp.mutex.Lock()
			wp.lastRound = status.LastRound
			wp.mutex.Unlock()
		}
		logger.Tracef("lastRound is %d", status.LastRound)
	}
}
