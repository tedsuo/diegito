package main

import (
	"math"
	"os"
	"path/filepath"
	"time"

	garden "github.com/cloudfoundry-incubator/garden/api"
	GardenClient "github.com/cloudfoundry-incubator/garden/client"
	GardenConnection "github.com/cloudfoundry-incubator/garden/client/connection"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/executor/cmd/executor/configuration"
	"github.com/cloudfoundry-incubator/executor/depot"
	"github.com/cloudfoundry-incubator/executor/depot/event"
	"github.com/cloudfoundry-incubator/executor/depot/metrics"
	"github.com/cloudfoundry-incubator/executor/depot/store"
	"github.com/cloudfoundry-incubator/executor/depot/tallyman"
	"github.com/cloudfoundry-incubator/executor/depot/transformer"
	"github.com/cloudfoundry-incubator/executor/depot/uploader"
	"github.com/cloudfoundry/dropsonde/emitter/logemitter"
	"github.com/cloudfoundry/gunk/timeprovider"
	"github.com/pivotal-golang/archiver/compressor"
	"github.com/pivotal-golang/archiver/extractor"
	"github.com/pivotal-golang/cacheddownloader"
	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/timer"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
)

const (
	tempDir                     = "/tmp"
	cachePath                   = "/tmp/cache"
	containerOwnerName          = "executor"
	containerMaxCpuShares       = 0
	containerInodeLimit         = 200000
	registryPruningInterval     = time.Minute
	healthyMonitoringInterval   = 30 * time.Second
	unhealthyMonitoringInterval = 500 * time.Millisecond
	maxCacheSizeInBytes         = 10 * 1024 * 1024 * 1024
	maxConcurrentDownloads      = 5
	maxConcurrentUploads        = 5
	memoryMBFlag                = configuration.Automatic
	diskMBFlag                  = configuration.Automatic
	allowPrivileged             = true
	skipCertVerify              = true
	exportNetworkEnvVars        = true
	metricsReportInterval       = 1 * time.Minute
	gardenSyncInterval          = 30 * time.Second
)

type executorContainers struct {
	gardenClient garden.Client
	owner        string
}

func (containers *executorContainers) Containers() ([]garden.Container, error) {
	return containers.gardenClient.Containers(garden.Properties{
		store.ContainerOwnerProperty: containers.owner,
	})
}

type ExecutorConfig struct {
	LoggregatorServer string
	LoggregatorSecret string
	GardenNetwork     string
	GardenAddr        string
}

func NewExecutor(logger lager.Logger, config ExecutorConfig) (ifrit.Runner, executor.Client) {
	gardenClient := GardenClient.New(GardenConnection.New(config.GardenNetwork, config.GardenAddr))
	waitForGarden(logger, gardenClient)

	containersFetcher := &executorContainers{
		gardenClient: gardenClient,
		owner:        containerOwnerName,
	}

	destroyContainers(gardenClient, containersFetcher, logger)

	workDir := setupWorkDir(logger, tempDir)

	transformer := initializeTransformer(
		logger,
		cachePath,
		workDir,
		maxCacheSizeInBytes,
		maxConcurrentDownloads,
		maxConcurrentUploads,
		allowPrivileged,
		skipCertVerify,
		exportNetworkEnvVars,
	)

	tallyman := tallyman.NewTallyman()

	hub := event.NewHub()

	gardenStore, allocationStore := initializeStores(
		logger,
		gardenClient,
		transformer,
		containerOwnerName,
		containerMaxCpuShares,
		containerInodeLimit,
		config.LoggregatorServer,
		config.LoggregatorSecret,
		registryPruningInterval,
		healthyMonitoringInterval,
		unhealthyMonitoringInterval,
		tallyman,
		hub,
	)

	depotClient := depot.NewClient(
		fetchCapacity(logger, gardenClient),
		gardenStore,
		allocationStore,
		tallyman,
		hub,
		logger,
	)

	group := grouper.NewOrdered(os.Interrupt, grouper.Members{
		{"metrics-reporter", &metrics.Reporter{
			ExecutorSource: depotClient,
			Interval:       metricsReportInterval,
			Logger:         logger.Session("metrics-reporter"),
		}},
		{"garden-syncer", gardenStore.TrackContainers(gardenSyncInterval)},
		{"hub-drainer", drainHub(hub)},
	})

	return group, depotClient
}

func drainHub(hub event.Hub) ifrit.Runner {
	return ifrit.RunFunc(func(signals <-chan os.Signal, ready chan<- struct{}) error {
		close(ready)
		<-signals
		hub.Close()
		return nil
	})
}

func setupWorkDir(logger lager.Logger, tempDir string) string {
	workDir := filepath.Join(tempDir, "executor-work")

	err := os.RemoveAll(workDir)
	if err != nil {
		logger.Error("working-dir.cleanup-failed", err)
		os.Exit(1)
	}

	err = os.MkdirAll(workDir, 0755)
	if err != nil {
		logger.Error("working-dir.create-failed", err)
		os.Exit(1)
	}

	return workDir
}

func initializeStores(
	logger lager.Logger,
	gardenClient garden.Client,
	transformer *transformer.Transformer,
	containerOwnerName string,
	containerMaxCpuShares uint64,
	containerInodeLimit uint64,
	loggregatorServer string,
	loggregatorSecret string,
	registryPruningInterval time.Duration,
	healthyMonitoringInterval time.Duration,
	unhealthyMonitoringInterval time.Duration,
	tallyman *tallyman.Tallyman,
	emitter store.EventEmitter,
) (*store.GardenStore, *store.AllocationStore) {
	os.Setenv("LOGGREGATOR_SHARED_SECRET", loggregatorSecret)
	logEmitter, err := logemitter.NewEmitter(loggregatorServer, "", "", false)
	if err != nil {
		panic(err)
	}

	gardenStore := store.NewGardenStore(
		logger.Session("garden-store"),
		gardenClient,
		containerOwnerName,
		containerMaxCpuShares,
		containerInodeLimit,
		healthyMonitoringInterval,
		unhealthyMonitoringInterval,
		logEmitter,
		transformer,
		timer.NewTimer(),
		tallyman,
		emitter,
	)

	allocationStore := store.NewAllocationStore(
		timeprovider.NewTimeProvider(),
		registryPruningInterval,
		tallyman,
		emitter,
	)

	return gardenStore, allocationStore
}

func initializeTransformer(
	logger lager.Logger,
	cachePath, workDir string,
	maxCacheSizeInBytes uint64,
	maxConcurrentDownloads, maxConcurrentUploads uint,
	allowPrivileged bool,
	skipSSLVerification bool,
	exportNetworkEnvVars bool,
) *transformer.Transformer {
	cache := cacheddownloader.New(cachePath, workDir, int64(maxCacheSizeInBytes), 10*time.Minute, int(math.MaxInt8), skipSSLVerification)
	uploader := uploader.New(10*time.Minute, skipSSLVerification, logger)
	extractor := extractor.NewDetectable()
	compressor := compressor.NewTgz()

	return transformer.NewTransformer(
		cache,
		uploader,
		extractor,
		compressor,
		make(chan struct{}, maxConcurrentDownloads),
		make(chan struct{}, maxConcurrentUploads),
		logger,
		workDir,
		allowPrivileged,
		exportNetworkEnvVars,
	)
}

func waitForGarden(logger lager.Logger, gardenClient GardenClient.Client) {
	err := gardenClient.Ping()

	for err != nil {
		logger.Error("failed-to-make-connection", err)
		time.Sleep(time.Second)
		err = gardenClient.Ping()
	}
}

func fetchCapacity(logger lager.Logger, gardenClient GardenClient.Client) executor.ExecutorResources {
	capacity, err := configuration.ConfigureCapacity(gardenClient, memoryMBFlag, diskMBFlag)
	if err != nil {
		logger.Error("failed-to-configure-capacity", err)
		os.Exit(1)
	}

	logger.Info("initial-capacity", lager.Data{
		"capacity": capacity,
	})

	return capacity
}

func destroyContainers(gardenClient garden.Client, containersFetcher *executorContainers, logger lager.Logger) {
	logger.Info("executor-fetching-containers-to-destroy")
	containers, err := containersFetcher.Containers()
	if err != nil {
		logger.Fatal("executor-failed-to-get-containers", err)
		return
	} else {
		logger.Info("executor-fetched-containers-to-destroy", lager.Data{"num-containers": len(containers)})
	}

	for _, container := range containers {
		logger.Info("executor-destroying-container", lager.Data{"container-handle": container.Handle()})
		err := gardenClient.Destroy(container.Handle())
		if err != nil {
			logger.Fatal("executor-failed-to-destroy-container", err, lager.Data{
				"handle": container.Handle(),
			})
		} else {
			logger.Info("executor-destroyed-stray-container", lager.Data{
				"handle": container.Handle(),
			})
		}
	}
}
