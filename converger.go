package main

import (
	"os"
	"time"

	"github.com/cloudfoundry-incubator/converger/converger_process"
	"github.com/cloudfoundry-incubator/converger/lrpwatcher"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/lock_bbs"
	"github.com/nu7hatch/gouuid"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
)

const (
	convergeRepeatInterval               = 30 * time.Second
	kickPendingTaskDuration              = 30 * time.Second
	expirePendingTaskDuration            = 30 * time.Minute
	expireCompletedTaskDuration          = 120 * time.Second
	kickPendingLRPStartAuctionDuration   = 30 * time.Second
	expireClaimedLRPStartAuctionDuration = 30 * time.Second
)

func NewConverger(bbs Bbs.ConvergerBBS, logger lager.Logger) ifrit.Runner {
	logger = logger.Session("converger")

	uuid, err := uuid.NewV4()
	if err != nil {
		logger.Fatal("Couldn't generate uuid", err)
	}

	heartbeater := bbs.NewConvergeLock(uuid.String(), lock_bbs.HEARTBEAT_INTERVAL)

	converger := converger_process.New(
		bbs,
		logger,
		convergeRepeatInterval,
		kickPendingTaskDuration,
		expirePendingTaskDuration,
		expireCompletedTaskDuration,
		kickPendingLRPStartAuctionDuration,
		expireClaimedLRPStartAuctionDuration,
	)

	watcher := lrpwatcher.New(bbs, logger)

	return grouper.NewOrdered(os.Interrupt, grouper.Members{
		{"heartbeater", heartbeater},
		{"converger", converger},
		{"watcher", watcher},
	})
}
