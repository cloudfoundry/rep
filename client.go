package rep

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/tedsuo/rata"
)

//go:generate counterfeiter -o repfakes/fake_client_factory.go . ClientFactory

type ClientFactory interface {
	CreateClient(address string) Client
}

type clientFactory struct {
	httpClient *http.Client
}

func NewClientFactory(httpClient *http.Client) ClientFactory {
	return &clientFactory{httpClient}
}

func (factory *clientFactory) CreateClient(address string) Client {
	return NewClient(factory.httpClient, address)
}

type AuctionCellClient interface {
	State() (CellState, error)
	Perform(work Work) (Work, error)
}

//go:generate counterfeiter -o repfakes/fake_client.go . Client

type Client interface {
	AuctionCellClient
	StopLRPInstance(key models.ActualLRPKey, instanceKey models.ActualLRPInstanceKey) error
	CancelTask(taskGuid string) error
}

//go:generate counterfeiter -o repfakes/fake_sim_client.go . SimClient

type SimClient interface {
	Client
	Reset() error
}

type client struct {
	client           *http.Client
	address          string
	requestGenerator *rata.RequestGenerator
}

func NewClient(httpClient *http.Client, address string) Client {
	return &client{
		client:           httpClient,
		address:          address,
		requestGenerator: rata.NewRequestGenerator(address, Routes),
	}
}

func (c *client) State() (CellState, error) {

	req, err := c.requestGenerator.CreateRequest(StateRoute, nil, nil)
	if err != nil {
		return CellState{}, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return CellState{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return CellState{}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var state CellState
	err = json.NewDecoder(resp.Body).Decode(&state)
	if err != nil {
		return CellState{}, err
	}

	return state, nil
}

func (c *client) Perform(work Work) (Work, error) {
	body, err := json.Marshal(work)
	if err != nil {
		return Work{}, err
	}

	req, err := c.requestGenerator.CreateRequest(PerformRoute, nil, bytes.NewReader(body))
	if err != nil {
		return Work{}, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return Work{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Work{}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var failedWork Work
	err = json.NewDecoder(resp.Body).Decode(&failedWork)
	if err != nil {
		return Work{}, err
	}

	return failedWork, nil
}

func (c *client) Reset() error {
	req, err := c.requestGenerator.CreateRequest(Sim_ResetRoute, nil, nil)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func (c *client) StopContainer(containerGuid string) error {
	req, err := c.requestGenerator.CreateRequest(StopContainerRoute, rata.Params{"container_guid": containerGuid}, nil)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("http error: status code %d (%s)", resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	return nil
}

func (c *client) StopLRPInstance(
	key models.ActualLRPKey,
	instanceKey models.ActualLRPInstanceKey,
) error {
	req, err := c.requestGenerator.CreateRequest(StopLRPInstanceRoute, stopParamsFromLRP(key, instanceKey), nil)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("http error: status code %d (%s)", resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	return nil
}

func (c *client) CancelTask(taskGuid string) error {
	req, err := c.requestGenerator.CreateRequest(CancelTaskRoute, rata.Params{"task_guid": taskGuid}, nil)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
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
