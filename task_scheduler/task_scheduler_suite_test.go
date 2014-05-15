package task_scheduler_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestTaskScheduler(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Task Scheduler Suite")
}
