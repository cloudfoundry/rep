package rep

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/cf_http"
	"github.com/tedsuo/rata"
)

//go:generate counterfeiter . CellClient

type CellClient interface {
	StopLRPInstance(cellURL string, key models.ActualLRPKey, instanceKey models.ActualLRPInstanceKey) error
	CancelTask(cellURL string, taskGuid string) error
}

type cellClient struct {
	httpClient *http.Client
}

func NewCellClient() CellClient {
	return &cellClient{
		httpClient: cf_http.NewClient(),
	}
}

func (c *cellClient) StopLRPInstance(
	cellURL string,
	key models.ActualLRPKey,
	instanceKey models.ActualLRPInstanceKey,
) error {
	reqGen := rata.NewRequestGenerator(cellURL, CellRoutes)

	req, err := reqGen.CreateRequest(StopLRPInstanceRoute, stopParamsFromLRP(key, instanceKey), nil)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("http error: status code %d (%s)", resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	return nil
}

func (c *cellClient) CancelTask(cellURL string, taskGuid string) error {
	reqGen := rata.NewRequestGenerator(cellURL, CellRoutes)

	req, err := reqGen.CreateRequest(CancelTaskRoute, rata.Params{"task_guid": taskGuid}, nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("http error: status code %d (%s)", resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	return nil
}

func stopParamsFromLRP(
	key models.ActualLRPKey,
	instanceKey models.ActualLRPInstanceKey,
) rata.Params {
	return rata.Params{
		"process_guid":  key.ProcessGuid,
		"instance_guid": instanceKey.InstanceGuid,
		"index":         strconv.Itoa(int(key.Index)),
	}
}
