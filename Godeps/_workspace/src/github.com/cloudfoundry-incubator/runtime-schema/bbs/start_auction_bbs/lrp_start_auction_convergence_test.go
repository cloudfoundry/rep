package start_auction_bbs_test

import (
	"path"
	"time"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry-incubator/runtime-schema/models"
	"github.com/cloudfoundry-incubator/runtime-schema/models/factories"
	"github.com/cloudfoundry/storeadapter"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	processGuid               = "process-guid"
	pendingKickDuration       = 30 * time.Second
	claimedExpirationDuration = 5 * time.Minute
)

var _ = Describe("LRPStartAuction Convergence", func() {
	var startAuctionEvents <-chan models.LRPStartAuction

	JustBeforeEach(func() {
		startAuctionEvents, _, _ = bbs.WatchForLRPStartAuction()
		bbs.ConvergeLRPStartAuctions(pendingKickDuration, claimedExpirationDuration)
	})

	Context("when the LRPAuction has invalid JSON", func() {
		var key = path.Join(shared.LRPStartAuctionSchemaRoot, "process-guid", "1")

		BeforeEach(func() {
			etcdClient.Create(storeadapter.StoreNode{
				Key:   key,
				Value: []byte("ß"),
			})
		})

		It("should be removed", func() {
			_, err := etcdClient.Get(key)
			Ω(err).Should(MatchError(storeadapter.ErrorKeyNotFound))
		})
	})

	Describe("Kicking pending auctions", func() {
		Context("up until the pending duration has passed", func() {
			BeforeEach(func() {
				newPendingStartAuction(processGuid)
				timeProvider.Increment(pendingKickDuration)
			})

			It("does not kick the auctions", func() {
				Consistently(startAuctionEvents).ShouldNot(Receive())
			})
		})

		Context("when the pending duration has passed", func() {
			var auction models.LRPStartAuction

			BeforeEach(func() {
				auction = newPendingStartAuction(processGuid)
				timeProvider.Increment(pendingKickDuration + time.Second)
				newPendingStartAuction(processGuid)
			})

			It("Only kicks auctions that haven't been updated in the given amount of time", func() {
				var noticedOnce models.LRPStartAuction
				Eventually(startAuctionEvents).Should(Receive(&noticedOnce))
				Ω(noticedOnce.Index).Should(Equal(auction.Index))

				Consistently(startAuctionEvents).ShouldNot(Receive())
			})
		})
	})

	Describe("Deleting very old claimed events", func() {
		Context("up until the claimedExpiration duration", func() {
			BeforeEach(func() {
				newClaimedStartAuction(processGuid)
				timeProvider.Increment(claimedExpirationDuration)
			})

			It("should not delete claimed events", func() {
				Ω(bbs.GetAllLRPStartAuctions()).Should(HaveLen(1))
			})
		})

		Context("when we are past the claimedExpiration duration", func() {
			BeforeEach(func() {
				newClaimedStartAuction(processGuid)
				newClaimedStartAuction("other-process")
				newClaimedStartAuction("process-to-delete")
				timeProvider.Increment(claimedExpirationDuration + 1*time.Second)
				newClaimedStartAuction(processGuid)
				newPendingStartAuction("other-process")
			})

			It("should delete claimed events that have expired", func() {
				Ω(bbs.GetAllLRPStartAuctions()).Should(HaveLen(2))
			})

			It("should prune start auction directories for events that have expired", func() {
				startedAuctionRoot, err := etcdClient.ListRecursively(shared.LRPStartAuctionSchemaRoot)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(startedAuctionRoot.ChildNodes).Should(HaveLen(2))
			})
		})
	})
})

var auctionIndex = 0

func newStartAuction(processGuid string) models.LRPStartAuction {
	auctionIndex += 1
	return models.LRPStartAuction{
		Index:        auctionIndex,
		InstanceGuid: factories.GenerateGuid(),

		DesiredLRP: models.DesiredLRP{
			Domain:      "tests",
			ProcessGuid: processGuid,
			Stack:       "some-stack",
			Instances:   1,
			Actions: []models.ExecutorAction{
				{
					Action: models.DownloadAction{
						From: "http://example.com",
						To:   "/tmp/internet",
					},
				},
			},
		},
	}
}

func newPendingStartAuction(processGuid string) models.LRPStartAuction {
	auction := newStartAuction(processGuid)

	err := bbs.RequestLRPStartAuction(auction)
	Ω(err).ShouldNot(HaveOccurred())
	auction.State = models.LRPStartAuctionStatePending
	auction.UpdatedAt = timeProvider.Time().UnixNano()

	return auction
}

func newClaimedStartAuction(processGuid string) models.LRPStartAuction {
	auction := newPendingStartAuction(processGuid)

	err := bbs.ClaimLRPStartAuction(auction)
	Ω(err).ShouldNot(HaveOccurred())
	auction.State = models.LRPStartAuctionStateClaimed
	auction.UpdatedAt = timeProvider.Time().UnixNano()

	return auction
}
