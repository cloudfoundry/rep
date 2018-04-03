package handlers_test

import (
	"errors"
	"net/http"

	"code.cloudfoundry.org/rep"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("State", func() {
	var (
		containerMetrics *rep.ContainerMetricsCollection
	)

	BeforeEach(func() {
		containerMetrics = &rep.ContainerMetricsCollection{
			CellID: "some-cell-id",
			LRPs: []rep.LRPMetric{
				{
					ProcessGUID:  "some-process-guid",
					InstanceGUID: "some-instance-guid",
				},
			},
			Tasks: []rep.TaskMetric{
				{
					TaskGUID: "some-guid",
				},
			},
		}
		fakeMetricCollector.MetricsReturns(containerMetrics, nil)
	})

	It("has the right field names", func() {
		status, body := Request(rep.ContainerMetricsRoute, nil, nil)
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(ContainSubstring(`process_guid`))
		Expect(body).To(ContainSubstring(`instance_guid`))
		Expect(body).To(ContainSubstring(`index`))
		Expect(body).To(ContainSubstring(`metric_guid`))
		Expect(body).To(ContainSubstring(`cpu_usage_fraction`))
		Expect(body).To(ContainSubstring(`disk_usage_bytes`))
		Expect(body).To(ContainSubstring(`disk_quota_bytes`))
		Expect(body).To(ContainSubstring(`memory_usage_bytes`))
		Expect(body).To(ContainSubstring(`memory_quota_bytes`))
		Expect(body).To(ContainSubstring(`task_guid`))
		Expect(body).To(ContainSubstring(`metric_guid`))
		Expect(body).To(ContainSubstring(`cpu_usage_fraction`))
		Expect(body).To(ContainSubstring(`disk_usage_bytes`))
		Expect(body).To(ContainSubstring(`disk_quota_bytes`))
		Expect(body).To(ContainSubstring(`memory_usage_bytes`))
		Expect(body).To(ContainSubstring(`memory_quota_bytes`))
	})

	It("it returns whatever the container_metrics call returns", func() {
		status, body := Request(rep.ContainerMetricsRoute, nil, nil)
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(MatchJSON(JSONFor(containerMetrics)))
		Expect(fakeMetricCollector.MetricsCallCount()).To(Equal(1))
	})

	Context("when the container_metrics call fails", func() {
		It("fails", func() {
			fakeMetricCollector.MetricsReturns(&rep.ContainerMetricsCollection{}, errors.New("boom"))
			Expect(fakeMetricCollector.MetricsCallCount()).To(Equal(0))

			status, body := Request(rep.ContainerMetricsRoute, nil, nil)
			Expect(status).To(Equal(http.StatusInternalServerError))
			Expect(body).To(BeEmpty())
			Expect(fakeMetricCollector.MetricsCallCount()).To(Equal(1))
		})
	})
})
