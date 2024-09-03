package importer

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
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

	windowLow  types.Round
	windowHigh types.Round

	numWorkers uint
}

// the worker pool assumes that the caller will fetch the blocks in order, starting from `initialRound`
func newWorkerPool(
	parentCtx context.Context,
	logger *logrus.Logger,
	numWorkers uint,
	initialRound types.Round,
) (*workerPool, error) {

	ctx, cancelFunc := context.WithCancel(parentCtx)

	client, err := algod.MakeClient("https://mainnet-api.algonode.cloud", "")
	if err != nil {
		logger.Error("failed to initialize algod client: ", err)
		cancelFunc()
		return nil, fmt.Errorf("failed to initialize algod: %w", err)
	}

	jobsCh := make(chan types.Round, numWorkers)
	roundsCh := make(chan types.Round, numWorkers)

	// spawn workers
	for i := uint(0); i < numWorkers; i++ {
		go workerEntrypoint(ctx, logger, client, jobsCh, roundsCh)
	}

	// create jobs to get the initial blocks
	for i := uint(0); i < numWorkers; i++ {
		jobsCh <- initialRound + types.Round(i)
	}

	go tipFollowerEntrypoint(ctx, logger, client)

	// return a handle to the worker pool
	wp := workerPool{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		client:     client,
		logger:     logger,
		jobsCh:     jobsCh,
		roundsCh:   roundsCh,
		//rounds: // zero value is valid
		windowLow:  initialRound,
		windowHigh: initialRound + types.Round(numWorkers),
		numWorkers: numWorkers,
	}
	return &wp, nil
}

func (wp *workerPool) close() {
	close(wp.jobsCh)
	wp.cancelFunc()
	//TODO close wp.roundsCh
	//TODO Do we wait until all workers exit gracefully? We probably don't care
}

func (wp *workerPool) getItem(rnd uint64) types.Round {

	if rnd != uint64(wp.windowLow) {
		wp.logger.Error("rnd != wp.windowLow", rnd, wp.windowLow)
		panic("")
	}

	for {
		// check if we already have the round
		wp.logger.Info("checking condition", wp.rounds.values)
		if wp.rounds.Len() != 0 && wp.rounds.Min() == uint64(wp.windowLow) {

			// take out the item
			tmp := wp.rounds.PopMin()
			wp.logger.Info("returning item ", tmp, wp.rounds.values)

			// update the sliding window size
			wp.windowLow++

			// create new jobs if needed (hence updating the window size)
			wp.advanceWindow()

			return types.Round(tmp)
		} else if wp.rounds.Len() != 0 {
			wp.logger.Info("item was not tip: ", wp.rounds.Min(), wp.rounds.values)
		}

		// wait for more data
		//wp.logger.Info("waiting on roundsCh")
		item := <-wp.roundsCh
		wp.rounds.Push(uint64(item))
	}

}

func (wp *workerPool) advanceWindow() {

	//TODO add additional check so we don't go past the chain tip
	if uint(wp.windowHigh-wp.windowLow) < wp.numWorkers {
		//wp.logger.Info("pushing job ", wp.windowHigh)
		wp.jobsCh <- wp.windowHigh
		wp.windowHigh++
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
) {
	var lastRoundSeen uint64 = 0

	for {
		// Wait for the next round
		// TODO Probably should add a timeout enforced by us here
		// TODO We could do an optimization here if the sliding window is too far away from the last round.
		status, err := client.StatusAfterBlock(lastRoundSeen).Do(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				logger.Info("tip follower leaving: context cancelled")
				return
			}
			logger.Error("failed to get status from algod: ", err)
			time.Sleep(5 * time.Second)
			continue
		}
		lastRoundSeen = status.LastRound

		//TODO update shared state
		logger.Info("current round is ", status.LastRound)
	}
}
