package main_test

import (
	//	. "code.cloudfoundry.org/lds/cmd/lds"

	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/config"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
)

var _ = Describe("Lds", func() {

	var (
		port          int
		fileContents  string
		listenersFile *os.File
		session       *gexec.Session
		ldsCommand    *exec.Cmd
	)

	var expectGetIndexToReturn = func(expectedContent string) {
		var resp *http.Response
		Eventually(func() error {
			ldsURL := fmt.Sprintf("http://127.0.0.1:%d", port)

			var err error
			resp, err = http.Get(ldsURL)
			return err

		}).Should(BeNil())

		body, err := ioutil.ReadAll(resp.Body)
		Expect(err).ToNot(HaveOccurred())

		resp.Body.Close()

		Expect(string(body)).To(Equal(expectedContent))

	}

	BeforeEach(func() {
		port = 62000 + config.GinkgoConfig.ParallelNode

		var err error
		listenersFile, err = ioutil.TempFile("", "listeners-config")
		Expect(err).ToNot(HaveOccurred())

		fileContents = `{"contents": "config"}`

		listenersFile.Write([]byte(fileContents))
		listenersFile.Sync()
		listenersFile.Close()

		ldsCommand = exec.Command(ldsBinPath, "-port="+strconv.Itoa(port), "-listener-config="+listenersFile.Name())
	})
	JustBeforeEach(func() {
		var err error
		session, err = gexec.Start(ldsCommand, GinkgoWriter, GinkgoWriter)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		session.Kill()
		os.Remove(listenersFile.Name())
	})

	It("returns the listeners in the listeners config", func() {
		expectGetIndexToReturn(fileContents)
	})

	Context("when the config file does not exist", func() {
		BeforeEach(func() {
			ldsCommand = exec.Command(ldsBinPath, "-port="+strconv.Itoa(port), "-listener-config=non-existant.txt")
		})

		It("exits with 1 and the server never comes up", func() {
			Eventually(session).Should(gexec.Exit(1))
		})

		It("prints a message to stderr", func() {
			Eventually(session.Err).Should(gbytes.Say("could not find container proxy listener config at 'non-existant.txt'"))
		})
	})

	Context("when the server fails to come up", func() {
		BeforeEach(func() {
			ldsCommand = exec.Command(ldsBinPath, "-port=70000", "-listener-config="+listenersFile.Name())
		})

		It("exits with 1 and the server never comes up", func() {
			Eventually(session).Should(gexec.Exit(1))
		})

		It("prints a message to stderr", func() {
			Eventually(session.Err).Should(gbytes.Say("container proxy listener discovery service failed to come up"))
		})
	})

	Context("when the config file changes", func() {
		It("serves the new content", func() {
			expectGetIndexToReturn(`{"contents": "config"}`)

			fileContents = `{"new-contents": "new-config"}`

			ioutil.WriteFile(listenersFile.Name(), []byte(fileContents), 0666)

			expectGetIndexToReturn(`{"new-contents": "new-config"}`)

		})
	})
})
