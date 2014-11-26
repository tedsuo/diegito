package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cloudfoundry-incubator/auction/communication/http/auction_http_handlers"
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/auction_cell_rep"
	"github.com/cloudfoundry-incubator/rep/gatherer"
	"github.com/cloudfoundry-incubator/rep/harvester"
	"github.com/cloudfoundry-incubator/rep/lrp_stopper"
	"github.com/cloudfoundry-incubator/rep/maintain"
	"github.com/cloudfoundry-incubator/rep/reaper"
	"github.com/cloudfoundry-incubator/rep/routes"
	"github.com/cloudfoundry-incubator/rep/stop_lrp_listener"
	"github.com/cloudfoundry-incubator/rep/task_scheduler"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gunk/localip"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/timer"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/http_server"
	"github.com/tedsuo/rata"
)

const (
	pollingInterval   = 30 * time.Second
	heartbeatInterval = 60 * time.Second
)

type RepConfig struct {
	CellID            string
	LRPHost           string
	Stack             string
	AuctionListenAddr string
}

func NewRep(bbs Bbs.RepBBS, logger lager.Logger, config RepConfig, executorClient executor.Client) ifrit.Runner {

	logger = logger.Session("rep")

	if !validateRepConfig(logger, config) {
		os.Exit(1)
	}

	removeActualLrpFromBBS(bbs, config.CellID, logger)

	lrpStopper := initializeLRPStopper(config.CellID, bbs, executorClient, logger)

	taskCompleter := reaper.NewTaskCompleter(bbs, logger)
	taskContainerReaper := reaper.NewTaskContainerReaper(executorClient, logger)
	actualLRPReaper := reaper.NewActualLRPReaper(bbs, logger)

	bulkProcessor, eventConsumer := initializeHarvesters(logger, pollingInterval, executorClient, bbs, config)

	gatherer := gatherer.NewGatherer(pollingInterval, timer.NewTimer(), []gatherer.Processor{
		bulkProcessor,
		taskCompleter,
		taskContainerReaper,
		actualLRPReaper,
	}, config.CellID, bbs, executorClient, logger)

	auctionServer, address := initializeAuctionServer(lrpStopper, bbs, executorClient, logger, config)

	return grouper.NewOrdered(os.Interrupt, grouper.Members{
		{"auction-server", auctionServer},
		{"heartbeater", initializeCellHeartbeat(address, bbs, executorClient, logger, config)},
		{"task-rep", initializeTaskRep(bbs, logger, executorClient, config)},
		{"stop-lrp-listener", initializeStopLRPListener(lrpStopper, bbs, logger)},
		{"gatherer", gatherer},
		{"event-consumer", eventConsumer},
	})
}

func validateRepConfig(logger lager.Logger, config RepConfig) bool {
	valid := true

	if config.CellID == "" {
		logger.Error("-cellID must be specified", nil)
		valid = false
	}

	if config.Stack == "" {
		logger.Error("-stack must be specified", nil)
		valid = false
	}

	if config.LRPHost == "" {
		logger.Error("-lrpHost must be specified", nil)
		valid = false
	}

	return valid
}

func removeActualLrpFromBBS(bbs Bbs.RepBBS, cellID string, logger lager.Logger) {
	for {
		lrps, err := bbs.ActualLRPsByCellID(cellID)
		if err != nil {
			logger.Error("failed-to-get-actual-lrps-by-cell-id", err, lager.Data{"cell-id": cellID})
			time.Sleep(time.Second)
			continue
		}

		for _, lrp := range lrps {
			err = bbs.RemoveActualLRP(lrp)
			if err != nil {
				logger.Error("failed-to-remove-actual-lrps", err, lager.Data{"cell-id": cellID, "actual-lrp": lrp, "total-lrps": len(lrps)})
				time.Sleep(time.Second)
				continue
			}
		}

		break
	}
}

func initializeTaskRep(bbs Bbs.RepBBS, logger lager.Logger, executorClient executor.Client, config RepConfig) *task_scheduler.TaskScheduler {
	return task_scheduler.New(config.CellID, bbs, logger, config.Stack, executorClient)
}

func initializeLRPStopper(guid string, bbs Bbs.RepBBS, executorClient executor.Client, logger lager.Logger) lrp_stopper.LRPStopper {
	return lrp_stopper.New(guid, bbs, executorClient, logger)
}

func initializeStopLRPListener(stopper lrp_stopper.LRPStopper, bbs Bbs.RepBBS, logger lager.Logger) ifrit.Runner {
	return stop_lrp_listener.New(stopper, bbs, logger)
}

func initializeHarvesters(
	logger lager.Logger,
	pollInterval time.Duration,
	executorClient executor.Client,
	bbs Bbs.RepBBS,
	config RepConfig,
) (gatherer.Processor, ifrit.Runner) {
	taskProcessor := harvester.NewTaskProcessor(
		logger,
		bbs,
		executorClient,
	)

	lrpProcessor := harvester.NewLRPProcessor(
		config.CellID,
		config.LRPHost,
		logger,
		bbs,
		executorClient,
	)

	containerProcessor := harvester.NewContainerProcessor(
		logger,
		taskProcessor,
		lrpProcessor,
	)

	bulkProcessor := harvester.NewBulkContainerProcessor(
		containerProcessor,
		logger,
	)

	eventConsumer := harvester.NewEventConsumer(
		logger,
		executorClient,
		containerProcessor,
	)

	return bulkProcessor, eventConsumer
}

func initializeAuctionServer(
	stopper lrp_stopper.LRPStopper,
	bbs Bbs.RepBBS,
	executorClient executor.Client,
	logger lager.Logger,
	config RepConfig,
) (ifrit.Runner, string) {
	auctionCellRep := auction_cell_rep.New(config.CellID, config.Stack, stopper, bbs, executorClient, logger)
	handlers := auction_http_handlers.New(auctionCellRep, logger)
	router, err := rata.NewRouter(routes.Routes, handlers)
	if err != nil {
		logger.Fatal("failed-to-construct-auction-router", err)
	}

	ip, err := localip.LocalIP()
	if err != nil {
		logger.Fatal("failed-to-fetch-ip", err)
	}

	port := strings.Split(config.AuctionListenAddr, ":")[1]
	address := fmt.Sprintf("http://%s:%s", ip, port)

	return http_server.New(config.AuctionListenAddr, router), address
}

func initializeCellHeartbeat(address string, bbs Bbs.RepBBS, executorClient executor.Client, logger lager.Logger, config RepConfig) ifrit.Runner {
	cellPresence := models.CellPresence{
		CellID:     config.CellID,
		RepAddress: address,
		Stack:      config.Stack,
	}

	heartbeat := bbs.NewCellHeartbeat(cellPresence, heartbeatInterval)
	return maintain.New(executorClient, heartbeat, logger, heartbeatInterval, timer.NewTimer())
}
