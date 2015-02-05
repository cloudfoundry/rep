package http_server

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/cloudfoundry-incubator/rep/evacuation/evacuation_context"
)

type EvacuationHandler struct {
	evacuatable evacuation_context.Evacuatable
}

func NewEvacuationHandler(evacuatable evacuation_context.Evacuatable) *EvacuationHandler {
	return &EvacuationHandler{
		evacuatable: evacuatable,
	}
}

func (h *EvacuationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.evacuatable.Evacuate()
	jsonBytes, err := json.Marshal(map[string]string{"ping_path": "/ping"})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(jsonBytes)))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write(jsonBytes)
}
