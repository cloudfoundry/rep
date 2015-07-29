package internal

import (
	"github.com/cloudfoundry-incubator/bbs"
	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context"
	legacybbs "github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/pivotal-golang/lager"
)

type lrpContainer struct {
	*models.ActualLRPKey
	*models.ActualLRPInstanceKey
	executor.Container
}

func newLRPContainer(lrpKey *models.ActualLRPKey, instanceKey *models.ActualLRPInstanceKey, container executor.Container) *lrpContainer {
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
	bbsClient bbs.Client,
	legacyBBS legacybbs.RepBBS,
	containerDelegate ContainerDelegate,
	cellID string,
	evacuationReporter evacuation_context.EvacuationReporter,
	evacuationTTLInSeconds uint64,
) LRPProcessor {
	ordinaryProcessor := newOrdinaryLRPProcessor(bbsClient, legacyBBS, containerDelegate, cellID)
	evacuationProcessor := newEvacuationLRPProcessor(legacyBBS, containerDelegate, cellID, evacuationTTLInSeconds)
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
