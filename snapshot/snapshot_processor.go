package snapshot

import (
	"errors"

	"github.com/pivotal-golang/lager"
)

var ErrInconceivableSnapshotType = errors.New("inconceivable snapshot type")

//go:generate counterfeiter -o fake_snapshot/fake_snapshot_processor.go . SnapshotProcessor

type SnapshotProcessor interface {
	Process(lager.Logger, Snapshot)
}

type snapshotProcessor struct {
	lrpProcessor  LRPProcessor
	taskProcessor TaskProcessor
}

func NewSnapshotProcessor(lrpProcessor LRPProcessor, taskProcessor TaskProcessor) SnapshotProcessor {
	return &snapshotProcessor{
		lrpProcessor:  lrpProcessor,
		taskProcessor: taskProcessor,
	}
}

func (p *snapshotProcessor) Process(logger lager.Logger, snap Snapshot) {
	logger = logger.Session("container-processor", lager.Data{"container-guid": snap.Guid()})

	logger.Debug("starting")
	defer logger.Debug("completed")

	switch snap := snap.(type) {
	case *LRPSnapshot:
		p.lrpProcessor.Process(logger, snap)

	case *TaskSnapshot:
		p.taskProcessor.Process(logger, snap)

	default:
		logger.Error("failed", ErrInconceivableSnapshotType)
	}
}
