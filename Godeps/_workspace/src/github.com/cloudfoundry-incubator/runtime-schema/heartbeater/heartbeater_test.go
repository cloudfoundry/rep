package heartbeater_test

import (
	"fmt"
	"os"
	"time"

	"github.com/cloudfoundry-incubator/runtime-schema/heartbeater"
	"github.com/cloudfoundry/storeadapter"
	"github.com/cloudfoundry/storeadapter/etcdstoreadapter"
	"github.com/cloudfoundry/storeadapter/workerpool"

	"github.com/pivotal-golang/lager"
	"github.com/pivotal-golang/lager/lagertest"
	"github.com/tedsuo/ifrit"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	heartbeatInterval = 100 * time.Millisecond
	heartbeatKey      = "/some-key"
	heartbeatValue    = "some-value"
)

var etcdClient storeadapter.StoreAdapter

var _ = Describe("Heartbeater", func() {
	var etcdProxy ifrit.Process
	var heart ifrit.Runner
	var logger lager.Logger
	var expectedHeartbeatNode = storeadapter.StoreNode{
		Key:   heartbeatKey,
		Value: []byte(heartbeatValue),
		TTL:   1,
	}

	BeforeEach(func() {
		etcdRunner.Stop()
		etcdRunner.Start()
		etcdProxy = ifrit.Envoke(proxyRunner)

		etcdClient = etcdstoreadapter.NewETCDStoreAdapter([]string{proxyUrl}, workerpool.NewWorkerPool(10))
		etcdClient.Connect()

		logger = lagertest.NewTestLogger("test")
		logger.RegisterSink(lager.NewWriterSink(GinkgoWriter, lager.DEBUG))

		heart = heartbeater.New(etcdClient, heartbeatKey, heartbeatValue, heartbeatInterval, logger)
	})

	AfterEach(func() {
		etcdProxy.Signal(os.Kill)
		Eventually(etcdProxy.Wait(), 3*time.Second).Should(Receive(BeNil()))
	})

	Context("when the node does not exist", func() {
		var heartBeat ifrit.Process

		BeforeEach(func() {
			heartBeat = ifrit.Envoke(heart)
		})

		AfterEach(func() {
			heartBeat.Signal(os.Kill)
			Eventually(heartBeat.Wait()).Should(Receive(BeNil()))
		})

		It("continuously writes the given node to the store", func() {
			Consistently(matchtHeartbeatNode(expectedHeartbeatNode), heartbeatInterval*4).ShouldNot(HaveOccurred())
		})

		Context("and it is sent a signal", func() {
			BeforeEach(func() {
				heartBeat.Signal(os.Interrupt)
			})

			It("exits and deletes the node from the store", func() {
				Eventually(heartBeat.Wait()).Should(Receive(BeNil()))
				_, err := etcdClient.Get(expectedHeartbeatNode.Key)
				Ω(err).Should(Equal(storeadapter.ErrorKeyNotFound))
			})
		})

		Context("and it is sent the kill signal", func() {
			BeforeEach(func() {
				heartBeat.Signal(os.Kill)
			})

			It("exits and does not delete the node from the store", func() {
				Eventually(heartBeat.Wait()).Should(Receive(BeNil()))
				Ω(matchtHeartbeatNode(expectedHeartbeatNode)()).ShouldNot(HaveOccurred())
			})
		})
	})

	Context("when the node is deleted after we have aquired a lock", func() {
		var heartBeat ifrit.Process

		BeforeEach(func() {
			heartBeat = ifrit.Envoke(heart)
			err := etcdClient.Delete(heartbeatKey)
			Ω(err).ShouldNot(HaveOccurred())
		})

		AfterEach(func() {
			heartBeat.Signal(os.Kill)
			Eventually(heartBeat.Wait()).Should(Receive())
		})

		It("re-creates the node", func() {
			Eventually(matchtHeartbeatNode(expectedHeartbeatNode)).Should(BeNil())
		})

		Describe("when there is a connection error", func() {
			BeforeEach(func() {
				etcdProxy.Signal(os.Kill)
				Eventually(etcdProxy.Wait(), 3*time.Second).Should(Receive(BeNil()))
			})

			It("retries until it succeeds", func() {
				restarted := make(chan struct{})
				go func() {
					time.Sleep(500 * time.Millisecond)
					etcdProxy = ifrit.Envoke(proxyRunner)
					close(restarted)
				}()

				Eventually(matchtHeartbeatNode(expectedHeartbeatNode)).Should(BeNil())
				<-restarted
			})

			Describe("when the TTL expires", func() {
				It("exits with an error", func() {
					Eventually(heartBeat.Wait(), 5*time.Second).Should(Receive(Equal(heartbeater.ErrStoreUnavailable)))
				})
			})
		})

		Describe("when someone else acquires the lock first", func() {
			var doppelNode storeadapter.StoreNode

			BeforeEach(func() {
				doppelNode = storeadapter.StoreNode{
					Key:   heartbeatKey,
					Value: []byte("doppel-value"),
				}

				err := etcdClient.Create(doppelNode)
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("does not write to the node", func() {
				Consistently(matchtHeartbeatNode(doppelNode), heartbeatInterval*2).Should(BeNil())
			})

			It("exits with an error", func() {
				Eventually(heartBeat.Wait(), 5*time.Second).Should(Receive(Equal(heartbeater.ErrLockFailed)))
			})
		})
	})

	Context("when we already have the lock", func() {
		var sigChan chan os.Signal
		var readyChan chan struct{}
		var doneChan chan struct{}

		BeforeEach(func() {
			sigChan = make(chan os.Signal, 1)
			readyChan = make(chan struct{})
			doneChan = make(chan struct{})

			heartBeat := ifrit.Envoke(heart)
			heartBeat.Signal(os.Kill)
			Eventually(heartBeat.Wait()).Should(Receive(BeNil()))

			go func() {
				heart.Run(sigChan, readyChan)
				close(doneChan)
			}()
		})

		AfterEach(func() {
			sigChan <- os.Kill
			Eventually(doneChan).Should(BeClosed())
		})

		It("becomes ready immediately", func() {
			select {
			case <-readyChan:
			case <-time.After(1 * time.Second):
				Fail("TTL expired before heartbeater became ready")
			}
		})

		It("continuously writes the given node to the store", func() {
			<-readyChan
			Consistently(matchtHeartbeatNode(expectedHeartbeatNode), heartbeatInterval*4).ShouldNot(HaveOccurred())
		})
	})

	Context("when someone else already has the lock", func() {
		var doppelNode storeadapter.StoreNode
		var heartbeatChan chan ifrit.Process

		BeforeEach(func() {
			doppelNode = storeadapter.StoreNode{
				Key:   heartbeatKey,
				Value: []byte("doppel-value"),
			}

			err := etcdClient.Create(doppelNode)
			Ω(err).ShouldNot(HaveOccurred())
		})

		JustBeforeEach(func() {
			heartbeatChan = make(chan ifrit.Process)
			go func() {
				heartbeatChan <- ifrit.Envoke(heart)
			}()
		})

		AfterEach(func() {
			err := etcdClient.Delete(heartbeatKey)
			Ω(err).ShouldNot(HaveOccurred())

			heartBeat := <-heartbeatChan
			heartBeat.Signal(os.Kill)
			Eventually(heartBeat.Wait()).Should(Receive(BeNil()))
		})

		Context("and the other maintainer does not go away", func() {
			It("does not overwrite the existing value", func() {
				Consistently(matchtHeartbeatNode(doppelNode), 2*heartbeatInterval).Should(BeNil())
			})
		})

		Context("and the other maintainer goes away", func() {
			BeforeEach(func() {
				go func() {
					time.Sleep(time.Second)
					err := etcdClient.Delete(heartbeatKey)
					Ω(err).ShouldNot(HaveOccurred())
				}()
			})

			It("starts heartbeating once it disappears", func() {
				Eventually(matchtHeartbeatNode(expectedHeartbeatNode), 2*time.Second).Should(BeNil())
			})
		})
	})

	Context("when we cannot connect to etcd", func() {
		var heartbeatChan chan ifrit.Process

		BeforeEach(func() {
			etcdProxy.Signal(os.Kill)
			Eventually(etcdProxy.Wait()).Should(Receive(BeNil()))

			heartbeatChan = make(chan ifrit.Process)
			go func() {
				heartbeatChan <- ifrit.Envoke(heart)
			}()
		})

		Context("and etcd eventually comes back", func() {
			var etcdProxyChan chan ifrit.Process

			BeforeEach(func() {
				etcdProxyChan = make(chan ifrit.Process)
				go func() {
					time.Sleep(heartbeatInterval)
					etcdProxyChan <- ifrit.Envoke(proxyRunner)
				}()
			})

			AfterEach(func() {
				etcdProxy = <-etcdProxyChan
				heartBeat := <-heartbeatChan
				heartBeat.Signal(os.Kill)
				Eventually(heartBeat.Wait()).Should(Receive(BeNil()))
			})

			It("begins heartbeating", func() {
				Eventually(matchtHeartbeatNode(expectedHeartbeatNode)).Should(BeNil())
			})
		})

		Context("and etcd never comes back", func() {
			AfterEach(func() {
				// we have to start etcd to ensure the blocked heartbeat does not pollute other tests
				etcdProxy = ifrit.Envoke(proxyRunner)
				var heartBeat ifrit.Process
				Eventually(heartbeatChan).Should(Receive(&heartBeat))
				heartBeat.Signal(os.Kill)
			})

			It("blocks on envoke forever", func() {
				Consistently(heartbeatChan, 2*heartbeatInterval).ShouldNot(Receive())
			})
		})
	})

	Context("when we lose our existing connection to etcd", func() {
		var heartBeat ifrit.Process

		BeforeEach(func() {
			heartBeat = ifrit.Envoke(heart)

			// simulate network partiion
			etcdProxy.Signal(os.Kill)
			Eventually(etcdProxy.Wait()).Should(Receive(BeNil()))
		})

		Context("and etcd comes back before the ttl expires", func() {
			BeforeEach(func() {
				time.Sleep(heartbeatInterval)
				etcdProxy = ifrit.Envoke(proxyRunner)
			})

			AfterEach(func() {
				heartBeat.Signal(os.Kill)
				Eventually(heartBeat.Wait()).Should(Receive(BeNil()))
			})

			It("resumes heartbeating", func() {
				Eventually(matchtHeartbeatNode(expectedHeartbeatNode), 2*time.Second).Should(BeNil())
			})
		})

		Context("and etcd does not come back before the ttl expires", func() {
			It("exits with an error", func() {
				Eventually(heartBeat.Wait(), 4*time.Second).Should(Receive(Equal(heartbeater.ErrStoreUnavailable)))
			})
		})
	})
})

func matchtHeartbeatNode(expectedNode storeadapter.StoreNode) func() error {
	return func() error {
		node, err := etcdClient.Get(expectedNode.Key)

		if err != nil {
			return err
		}

		if node.Key != expectedNode.Key {
			return fmt.Errorf("Key not matching: %s : %s", node.Key, expectedNode.Key)
		}

		if string(node.Value) != string(expectedNode.Value) {
			return fmt.Errorf("Value not matching: %s : %s", node.Value, expectedNode.Value)
		}

		if node.TTL != expectedNode.TTL {
			return fmt.Errorf("TTL not matching: %s : %s", node.TTL, expectedNode.TTL)
		}

		return nil
	}
}
