package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/cloudfoundry-incubator/natbeat"
	"github.com/cloudfoundry-incubator/receptor/handlers"
	"github.com/cloudfoundry-incubator/receptor/task_watcher"
	Bbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry/gunk/localip"
	"github.com/pivotal-golang/lager"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/http_server"
)

const (
	corsEnabled        = true
	registerWithRouter = true
)

type ReceptorConfig struct {
	ServerDomainNames string
	ServerAddress     string
	Username          string
	Password          string
	NatsAddresses     string
	NatsUsername      string
	NatsPassword      string
}

func NewReceptor(bbs Bbs.ReceptorBBS, logger lager.Logger, config ReceptorConfig) ifrit.Runner {
	logger = logger.Session("receptor")

	if err := validateNatsArguments(config); err != nil {
		logger.Error("invalid-nats-flags", err)
		os.Exit(1)
	}

	handler := handlers.New(bbs, logger, config.Username, config.Password, corsEnabled)

	members := grouper.Members{
		{"server", http_server.New(config.ServerAddress, handler)},
		{"watcher", task_watcher.New(bbs, logger)},
	}

	if registerWithRouter {
		registration := initializeServerRegistration(logger, config)

		members = append(members, grouper.Member{
			Name:   "background_heartbeat",
			Runner: natbeat.NewBackgroundHeartbeat(config.NatsAddresses, config.NatsUsername, config.NatsPassword, logger, registration),
		})
	}
	return grouper.NewOrdered(os.Interrupt, members)
}

func validateNatsArguments(config ReceptorConfig) error {
	if registerWithRouter {
		if config.NatsAddresses == "" || config.ServerDomainNames == "" {
			return errors.New("registerWithRouter is set, but nats addresses or domain names were left blank")
		}
	}
	return nil
}

func initializeServerRegistration(logger lager.Logger, config ReceptorConfig) (registration natbeat.RegistryMessage) {
	domains := strings.Split(config.ServerDomainNames, ",")

	addressComponents := strings.Split(config.ServerAddress, ":")
	if len(addressComponents) != 2 {
		logger.Error("server-address-invalid", fmt.Errorf("%s is not a valid serverAddress", config.ServerAddress))
		os.Exit(1)
	}

	host, err := localip.LocalIP()
	if err != nil {
		logger.Error("local-ip-invalid", err)
		os.Exit(1)
	}

	port, err := strconv.Atoi(addressComponents[1])
	if err != nil {
		logger.Error("server-address-invalid", fmt.Errorf("%s does not have a valid port", config.ServerAddress))
		os.Exit(1)
	}

	return natbeat.RegistryMessage{
		URIs: domains,
		Host: host,
		Port: port,
	}
}
