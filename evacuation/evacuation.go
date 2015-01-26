package evacuation

import (
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pivotal-golang/clock"
	"github.com/pivotal-golang/lager"
)

//go:generate counterfeiter -o fake_evacuator/fake_evacuator.go . EvacuationContext
type EvacuationContext interface {
	Evacuating() bool
}

type evacuationContext struct {
	evacuating int32
}

func (e *evacuationContext) Evacuating() bool {
	return atomic.LoadInt32(&e.evacuating) != 0
}

type Evacuator struct {
	evacuationContext evacuationContext
	evacuationTimeout time.Duration
	clock             clock.Clock
}

func NewEvacuator(logger lager.Logger, clock clock.Clock, evacuationTimeout time.Duration) *Evacuator {
	return &Evacuator{
		evacuationContext: evacuationContext{},
		evacuationTimeout: evacuationTimeout,
		clock:             clock,
	}
}

func (e *Evacuator) EvacuationContext() EvacuationContext {
	return &e.evacuationContext
}

func (e *Evacuator) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	close(ready)

	select {
	case signal := <-signals:
		if signal == syscall.SIGUSR1 {
			atomic.AddInt32(&e.evacuationContext.evacuating, 1)

			timer := e.clock.NewTimer(e.evacuationTimeout)
			<-timer.C()
		}
	}

	return nil
}
