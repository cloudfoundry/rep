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
	bbs      bbs.RepBBS
	executor executorclient.Client
	lrpHost  string
	logger   *gosteno.Logger
}

func NewHandler(bbs bbs.RepBBS, executor executorclient.Client, lrpHost string, logger *gosteno.Logger) http.Handler {
	return &handler{
		bbs:      bbs,
		executor: executor,
		lrpHost:  lrpHost,
		logger:   logger,
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

	lrp := models.LRP{
		ProcessGuid:  processGuid,
		Index:        index,
		InstanceGuid: instanceGuid,

		Host:  handler.lrpHost,
		Ports: ports,
	}

	err = handler.bbs.ReportLongRunningProcessAsRunning(lrp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
