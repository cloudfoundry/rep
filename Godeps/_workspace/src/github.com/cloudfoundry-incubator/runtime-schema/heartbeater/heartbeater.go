package heartbeater

import (
	"errors"
	"math"
	"os"
	"time"

	"github.com/cloudfoundry/storeadapter"
	"github.com/pivotal-golang/lager"
)

var (
	ErrLockFailed       = errors.New("failed to compare and swap")
	ErrStoreUnavailable = errors.New("failed to connect to etcd")
)

type Heartbeater struct {
	Client   storeadapter.StoreAdapter
	Key      string
	Value    string
	Interval time.Duration
	Logger   lager.Logger
}

func New(etcdClient storeadapter.StoreAdapter, heartbeatKey string, heartbeatValue string, heartbeatInterval time.Duration, logger lager.Logger) Heartbeater {
	return Heartbeater{
		Client:   etcdClient,
		Key:      heartbeatKey,
		Value:    heartbeatValue,
		Interval: heartbeatInterval,
		Logger:   logger,
	}
}

func (h Heartbeater) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	logger := h.Logger.Session("heartbeat", lager.Data{"key": h.Key, "value": h.Value})

	ttl := uint64(math.Ceil((h.Interval * 2).Seconds()))

	node := storeadapter.StoreNode{
		Key:   h.Key,
		Value: []byte(h.Value),
		TTL:   ttl,
	}

	logger.Info("starting")

	err := h.Client.CompareAndSwap(node, node)
	if err != nil {
		logger.Info("initial-compare-and-swap-failed", lager.Data{"error": err.Error()})
		for {
			err := h.Client.Create(node)
			if err == nil {
				break
			}
			logger.Info("create-failed", lager.Data{"error": err.Error()})
			time.Sleep(h.Interval)
		}
	}

	logger.Info("started")
	close(ready)

	var connectionTimeout <-chan time.Time

	for {
		select {
		case sig := <-signals:
			switch sig {
			case os.Kill:
				return nil
			default:
				h.Client.CompareAndDelete(node)
				return nil
			}

		case <-connectionTimeout:
			logger.Info("connection-timed-out")
			return ErrStoreUnavailable

		case <-time.After(h.Interval):
			err := h.Client.CompareAndSwap(node, node)
			logger.Info("compare-and-swap")
			switch err {
			case storeadapter.ErrorTimeout:
				logger.Error("store-timeout", err)
				if connectionTimeout == nil {
					connectionTimeout = time.After(time.Duration(ttl * uint64(time.Second)))
				}
			case storeadapter.ErrorKeyNotFound:
				err := h.Client.Create(node)
				if err != nil && connectionTimeout == nil {
					connectionTimeout = time.After(time.Duration(ttl * uint64(time.Second)))
				}
			case nil:
				connectionTimeout = nil
			default:
				logger.Error("compare-and-swap-failed", err)
				return ErrLockFailed
			}
		}
	}
}
