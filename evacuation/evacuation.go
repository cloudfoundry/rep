package evacuation

import (
	"os"
	"sync/atomic"
	"syscall"

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
}

func NewEvacuator(logger lager.Logger) *Evacuator {
	return &Evacuator{
		evacuationContext: evacuationContext{},
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
		}
		return nil
	}

	return nil
}
