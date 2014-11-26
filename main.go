package main

import (
	"flag"
	"os"
	"strings"

	"github.com/cloudfoundry-incubator/cf-debug-server"
	"github.com/cloudfoundry-incubator/cf-lager"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/cloudfoundry/gunk/workpool"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/sigmon"
)

// etcd

var etcdCluster = flag.String(
	"etcdCluster",
	"http://127.0.0.1:4001",
	"comma-separated list of etcd addresses (http://ip:port)",
)

// garden

var gardenNetwork = flag.String(
	"gardenNetwork",
	"unix",
	"network mode for garden server (tcp, unix)",
)

var gardenAddr = flag.String(
	"gardenAddr",
	"/tmp/garden.sock",
	"network address for garden server",
)

// loggregator

var loggregatorServer = flag.String(
	"loggregatorServer",
	"",
	"loggregator server to emit logs to",
)

var loggregatorSecret = flag.String(
	"loggregatorSecret",
	"",
	"secret for the loggregator server",
)

// NATS

var natsAddresses = flag.String(
	"natsAddresses",
	"",
	"Comma-separated list of NATS addresses (ip:port).",
)

var natsUsername = flag.String(
	"natsUsername",
	"",
	"Username to connect to nats.",
)

var natsPassword = flag.String(
	"natsPassword",
	"",
	"Password for nats user.",
)

// receptor config

var receptorDomainNames = flag.String(
	"receptorDomainNames",
	"",
	"Comma separated list of domains that should route to the receptor server.",
)

var receptorAddress = flag.String(
	"receptorAddress",
	"",
	"The host:port that the receptor server is bound to.",
)

var receptorUsername = flag.String(
	"receptorUsername",
	"",
	"Username for receptor basic auth, enables basic auth if set.",
)

var receptorPassword = flag.String(
	"receptorPassword",
	"",
	"Password for receptor basic auth.",
)

// rep config

var auctionListenAddr = flag.String(
	"auctionListenAddr",
	"0.0.0.0:1800",
	"host:port to serve auction requests on",
)

var lrpHost = flag.String(
	"lrpHost",
	"",
	"address to route traffic to for LRP access",
)

var stack = flag.String(
	"stack",
	"",
	"the rep stack - must be specified",
)

var cellID = flag.String(
	"cellID",
	"",
	"the ID used by the rep to identify itself to external systems - must be specified",
)

func init() {
	flag.Parse()
	cf_debug_server.Run()
}

func main() {
	logger := cf_lager.New("diegito")
	bbs := newBBS(logger)

	executorConfig := ExecutorConfig{
		LoggregatorServer: *loggregatorServer,
		LoggregatorSecret: *loggregatorSecret,
		GardenNetwork:     *gardenNetwork,
		GardenAddr:        *gardenAddr,
	}

	receptorConfig := ReceptorConfig{
		ServerDomainNames: *receptorDomainNames,
		ServerAddress:     *receptorAddress,
		Username:          *receptorUsername,
		Password:          *receptorPassword,
		NatsAddresses:     *natsAddresses,
		NatsUsername:      *natsUsername,
		NatsPassword:      *natsPassword,
	}

	repConfig := RepConfig{
		AuctionListenAddr: *auctionListenAddr,
		CellID:            *cellID,
		LRPHost:           *lrpHost,
		Stack:             *stack,
	}

	executor, executorClient := NewExecutor(logger, executorConfig)

	diegoGroup := grouper.NewParallel(os.Interrupt, grouper.Members{
		{"auctioneer", NewAuctioneer(bbs, logger)},
		{"converger", NewConverger(bbs, logger)},
		{"executor", executor},
		{"receptor", NewReceptor(bbs, logger, receptorConfig)},
		{"rep", NewRep(bbs, logger, repConfig, executorClient)},
	})

	monitor := ifrit.Invoke(sigmon.New(diegoGroup))

	logger.Info("diegito started")

	err := <-monitor.Wait()
	if err != nil {
		logger.Error("exited-with-failure", err)
		os.Exit(1)
	}

	logger.Info("exited")
}

func newBBS(logger lager.Logger) *Bbs.BBS {
	etcdAdapter := newEtcdAdapter(logger)
	return Bbs.NewBBS(etcdAdapter, timeprovider.NewTimeProvider(), logger)
}

func newEtcdAdapter(logger lager.Logger) *etcdstoreadapter.ETCDStoreAdapter {
	etcdAdapter := etcdstoreadapter.NewETCDStoreAdapter(
		strings.Split(*etcdCluster, ","),
		workpool.NewWorkPool(10),
	)

	err := etcdAdapter.Connect()
	if err != nil {
		logger.Fatal("failed-to-connect-to-etcd", err)
	}

	return etcdAdapter
}
