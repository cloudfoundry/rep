package lrprunning

import (
	"net/http"
	"strconv"

	executorclient "github.com/cloudfoundry-incubator/executor/client"
	"github.com/cloudfoundry-incubator/runtime-schema/bbs"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry/gosteno"
)

type handler struct {
	executorID string
	bbs        bbs.RepBBS
	executor   executorclient.Client
	lrpHost    string
	logger     *gosteno.Logger
}

func NewHandler(executorID string, bbs bbs.RepBBS, executor executorclient.Client, lrpHost string, logger *gosteno.Logger) http.Handler {
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

	identifier := models.LRPIdentifier{
		ProcessGuid:  processGuid,
		Index:        index,
		InstanceGuid: instanceGuid,
	}

	container, err := handler.executor.GetContainer(identifier.OpaqueID())

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

	handler.logger.Infod(map[string]interface{}{
		"actual": lrp,
	}, "rep.lrp-running-handler.marking-actual-as-running")
	err = handler.bbs.ReportActualLRPAsRunning(lrp, handler.executorID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
