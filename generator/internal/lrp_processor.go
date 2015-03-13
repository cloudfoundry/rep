package internal

import (
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

type lrpContainer struct {
	models.ActualLRPKey
	models.ActualLRPInstanceKey
	executor.Container
}

func newLRPContainer(lrpKey models.ActualLRPKey, instanceKey models.ActualLRPInstanceKey, container executor.Container) *lrpContainer {
	return &lrpContainer{
		ActualLRPKey:         lrpKey,
		ActualLRPInstanceKey: instanceKey,
		Container:            container,
	}
}

//go:generate counterfeiter -o fake_internal/fake_lrp_processor.go lrp_processor.go LRPProcessor

type LRPProcessor interface {
	Process(lager.Logger, executor.Container)
}

type lrpProcessor struct {
	evacuationReporter  evacuation_context.EvacuationReporter
	ordinaryProcessor   LRPProcessor
	evacuationProcessor LRPProcessor
}

func NewLRPProcessor(
	bbs bbs.RepBBS,
	containerDelegate ContainerDelegate,
	cellID string,
	evacuationReporter evacuation_context.EvacuationReporter,
	evacuationTTLInSeconds uint64,
) LRPProcessor {
	ordinaryProcessor := newOrdinaryLRPProcessor(bbs, containerDelegate, cellID)
	evacuationProcessor := newEvacuationLRPProcessor(bbs, containerDelegate, cellID, evacuationTTLInSeconds)
	return &lrpProcessor{
		evacuationReporter:  evacuationReporter,
		ordinaryProcessor:   ordinaryProcessor,
		evacuationProcessor: evacuationProcessor,
	}
}

func (p *lrpProcessor) Process(logger lager.Logger, container executor.Container) {
	if p.evacuationReporter.Evacuating() {
		p.evacuationProcessor.Process(logger, container)
	} else {
		p.ordinaryProcessor.Process(logger, container)
	}
}
