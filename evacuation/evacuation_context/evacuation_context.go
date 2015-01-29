package evacuation_context

//go:generate counterfeiter -o fake_evacuation_context/fake_evacuatable.go . Evacuatable
type Evacuatable interface {
	Evacuate()
}

//go:generate counterfeiter -o fake_evacuation_context/fake_evacuation_reporter.go . EvacuationReporter
type EvacuationReporter interface {
	Evacuating() bool
}

type evacuationContext struct {
	evacuated chan struct{}
}

func New() (Evacuatable, EvacuationReporter) {
	evacuationContext := &evacuationContext{
		evacuated: make(chan struct{}),
	}

	return evacuationContext, evacuationContext
}

func (e *evacuationContext) Evacuating() bool {
	select {
	case <-e.evacuated:
		return true
	default:
		return false
	}
}

func (e *evacuationContext) Evacuate() {
	select {
	case <-e.evacuated:
	default:
		close(e.evacuated)
	}
}
