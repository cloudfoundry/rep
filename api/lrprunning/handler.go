package lrprunning

import (
	"net/http"
	"strconv"

	"github.com/cloudfoundry-incubator/executor"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/pivotal-golang/lager"
)

type handler struct {
	executorID string
	bbs        bbs.RepBBS
	executor   executor.Client
	lrpHost    string
	logger     lager.Logger
}

func NewHandler(
	executorID string,
	bbs bbs.RepBBS,
	executor executor.Client,
	lrpHost string,
	logger lager.Logger,
) http.Handler {
	return &handler{
		executorID: executorID,
		bbs:        bbs,
		executor:   executor,
		lrpHost:    lrpHost,
		logger:     logger,
	}
}

func (handler *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	processGuid := r.FormValue(":process_guid")

	indexStr := r.FormValue(":index")
	index, err := strconv.Atoi(indexStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	instanceGuid := r.FormValue(":instance_guid")

	container, err := handler.executor.GetContainer(instanceGuid)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ports := []models.PortMapping{}
	for _, portMapping := range container.Ports {
		ports = append(ports, models.PortMapping{
			ContainerPort: portMapping.ContainerPort,
			HostPort:      portMapping.HostPort,
		})
	}

	lrp := models.ActualLRP{
		ProcessGuid:  processGuid,
		Index:        index,
		InstanceGuid: instanceGuid,

		Host:  handler.lrpHost,
		Ports: ports,
	}

	handler.logger.Info("marking-actual-as-running", lager.Data{
		"actual": lrp,
	})

	err = handler.bbs.ReportActualLRPAsRunning(lrp, handler.executorID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
