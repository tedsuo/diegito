package main

import (
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cloudfoundry-incubator/auction/auctionrunner"
	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/auctioneer/auctioninbox"
	"github.com/cloudfoundry-incubator/auctioneer/auctionrunnerdelegate"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/lock_bbs"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/gunk/workpool"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	"github.com/nu7hatch/gouuid"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
)

const (
	communicationTimeout      = 10 * time.Second
	maxRetries                = 5
	communicationWorkPoolSize = 1000
)

func NewAuctioneer(bbs Bbs.AuctioneerBBS, logger lager.Logger) ifrit.Runner {
	logger = logger.Session("auctioneer")
	auctionRunner := initializeAuctionRunner(bbs, logger)
	auctionInbox := initializeAuctionInbox(auctionRunner, bbs, logger)

	uuid, err := uuid.NewV4()
	if err != nil {
		logger.Fatal("Couldn't generate uuid", err)
	}
	heartbeater := bbs.NewAuctioneerLock(uuid.String(), lock_bbs.HEARTBEAT_INTERVAL)

	return grouper.NewOrdered(os.Interrupt, grouper.Members{
		{"heartbeater", heartbeater},
		{"auction-runner", auctionRunner},
		{"auction-inbox", auctionInbox},
	})
}

func initializeAuctionRunner(bbs Bbs.AuctioneerBBS, logger lager.Logger) auctiontypes.AuctionRunner {
	httpClient := &http.Client{
		Timeout:   communicationTimeout,
		Transport: &http.Transport{},
	}

	delegate := auctionrunnerdelegate.New(httpClient, bbs, logger)
	return auctionrunner.New(delegate, timeprovider.NewTimeProvider(), maxRetries, workpool.NewWorkPool(communicationWorkPoolSize), logger)
}

func initializeAuctionInbox(runner auctiontypes.AuctionRunner, bbs Bbs.AuctioneerBBS, logger lager.Logger) *auctioninbox.AuctionInbox {
	return auctioninbox.New(runner, bbs, logger)
}

func initializeBBS(logger lager.Logger) Bbs.AuctioneerBBS {
	etcdAdapter := etcdstoreadapter.NewETCDStoreAdapter(
		strings.Split(*etcdCluster, ","),
		workpool.NewWorkPool(10),
	)

	err := etcdAdapter.Connect()
	if err != nil {
		logger.Fatal("failed-to-connect-to-etcd", err)
	}

	return Bbs.NewAuctioneerBBS(etcdAdapter, timeprovider.NewTimeProvider(), logger)
}
