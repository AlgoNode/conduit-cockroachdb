package importer

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/algorand/go-algorand-sdk/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/v2/types"
	"github.com/sirupsen/logrus"
)

type workerPool struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	logger *logrus.Logger

	client *algod.Client

	jobsCh chan<- types.Round

	roundsCh <-chan types.Round
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

	jobsCh := make(chan types.Round, numWorkers)
	roundsCh := make(chan types.Round, numWorkers)

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

		//rounds: // zero value is valid
		//mutex: // zero value is valid

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
	//TODO close wp.roundsCh
	//TODO Do we wait until all workers exit gracefully? We probably don't care
}

func (wp *workerPool) getItem(rnd uint64) types.Round {

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
			wp.logger.Infof("checking whether round %d is already downloaded: %d", rnd, wp.rounds.values)
			if wp.rounds.Len() != 0 && wp.rounds.Min() == uint64(wp.windowLow) {

				// take out the item
				tmp := wp.rounds.PopMin()
				wp.logger.Infof("returning round %d (%d)", tmp, wp.rounds.values)

				// update the sliding window size
				wp.windowLow++

				// create new jobs if needed (hence updating the window size)
				wp.advanceWindow()

				wp.mutex.Unlock()
				return types.Round(tmp)
			}
			wp.mutex.Unlock()
		}

		// wait for more data
		//wp.logger.Info("waiting on roundsCh")
		item := <-wp.roundsCh
		wp.rounds.Push(uint64(item))
	}

}

func (wp *workerPool) advanceWindow() {

	wp.logger.Infof("updating sliding window windowHigh=%d windowLow=%d numWorkers=%d", wp.windowHigh, wp.windowLow, wp.numWorkers)

	// Make sure the sliding window doesn't go past the chain tip
	if wp.windowHigh <= wp.lastRound {

		// if the sliding window is smaller than NUM_WORKERS, then advance it
		if (wp.windowHigh - wp.windowLow) < wp.numWorkers {

			steps := wp.numWorkers - (wp.windowHigh - wp.windowLow)
			wp.logger.Infof("creating %d jobs", steps)

			for i := uint64(0); i < steps; i++ {
				wp.jobsCh <- types.Round(wp.windowHigh)
				wp.windowHigh++
			}
		}

	}

}

func workerEntrypoint(
	_ context.Context,
	logger *logrus.Logger,
	client *algod.Client,
	jobsCh <-chan types.Round,
	roundsCh chan<- types.Round,
) {

	for {
		round := <-jobsCh

		// TODO Fetch the block from the network
		time.Sleep(time.Duration(rand.IntN(5)) * time.Second)

		roundsCh <- round
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
		// TODO Probably should add a timeout enforced by us here
		// TODO We could do an optimization here if the sliding window is too far away from the last round.
		status, err := client.StatusAfterBlock(lastRound).Do(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				logger.Info("tip follower leaving: context cancelled")
				return
			}
			logger.Errorf("failed to get status from algod: %s", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// set the blockchain's last known round
		{
			wp.mutex.Lock()
			wp.lastRound = status.LastRound
			wp.mutex.Unlock()
		}
		logger.Infof("lastRound is %d", status.LastRound)
	}
}
