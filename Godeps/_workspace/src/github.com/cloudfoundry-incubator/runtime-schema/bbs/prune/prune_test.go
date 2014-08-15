package prune_test

import (
	"github.com/cloudfoundry-incubator/runtime-schema/bbs/prune"
	db "github.com/cloudfoundry/storeadapter"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Prune", func() {
	var keysSetsToDelete [][]string
	var expectedKeysSetsToDelete [][]string
	var exampleTree db.StoreNode

	JustBeforeEach(func() {
		pruner := prune.NewPruner(exampleTree, func(node db.StoreNode) bool {
			return string(node.Value) == "true"
		})

		keysSetsToDelete = pruner.Prune()
	})

	Context("an empty tree", func() {
		BeforeEach(func() {
			expectedKeysSetsToDelete = [][]string{
				{"/0"},
				{"/0/0", "/0/1"},
				{"/0/0/0", "/0/1/0"},
			}
			exampleTree = db.StoreNode{
				Key: "/0",
				Dir: true,
				ChildNodes: []db.StoreNode{
					{
						Key: "/0/0",
						Dir: true,
						ChildNodes: []db.StoreNode{
							{
								Key:        "/0/0/0",
								Dir:        true,
								ChildNodes: []db.StoreNode{},
							},
						},
					},
					{
						Key: "/0/1",
						Dir: true,
						ChildNodes: []db.StoreNode{
							{
								Key: "/0/1/0",
								Dir: true,
							},
						},
					},
				},
			}
		})

		It("deletes the correct keys", func() {
			Ω(keysSetsToDelete).Should(Equal(expectedKeysSetsToDelete))
		})
	})

	Context("a tree filled with deletables", func() {
		BeforeEach(func() {
			expectedKeysSetsToDelete = [][]string{
				{"/0"},
				{"/0/0", "/0/1"},
				{"/0/0/0", "/0/1/0"},
				{"/0/0/0/0", "/0/1/0/0"},
			}

			exampleTree = db.StoreNode{
				Key: "/0",
				Dir: true,
				ChildNodes: []db.StoreNode{
					{
						Key: "/0/0",
						Dir: true,
						ChildNodes: []db.StoreNode{
							{
								Key: "/0/0/0",
								Dir: true,
								ChildNodes: []db.StoreNode{
									{
										Key:   "/0/0/0/0",
										Value: []byte("false"),
									},
								},
							},
						},
					},
					{
						Key: "/0/1",
						Dir: true,
						ChildNodes: []db.StoreNode{
							{
								Key: "/0/1/0",
								Dir: true,
								ChildNodes: []db.StoreNode{
									{
										Key:   "/0/1/0/0",
										Value: []byte("false"),
									},
								},
							},
						},
					},
				},
			}
		})

		It("deletes the correct keys", func() {
			Ω(keysSetsToDelete).Should(Equal(expectedKeysSetsToDelete))
		})
	})

	Context("a tree filled with keepables", func() {
		BeforeEach(func() {
			expectedKeysSetsToDelete = [][]string{
				{},
				{},
				{},
				{},
			}

			exampleTree = db.StoreNode{
				Key: "/0",
				Dir: true,
				ChildNodes: []db.StoreNode{
					{
						Key: "/0/0",
						Dir: true,
						ChildNodes: []db.StoreNode{
							{
								Key: "/0/0/0",
								Dir: true,
								ChildNodes: []db.StoreNode{
									{
										Key:   "/0/1/0/0",
										Value: []byte("true"),
									},
								},
							},
						},
					},
					{
						Key: "/0/1",
						Dir: true,
						ChildNodes: []db.StoreNode{
							{
								Key: "/0/1/0",
								Dir: true,
								ChildNodes: []db.StoreNode{
									{
										Key:   "/0/1/0/0",
										Value: []byte("true"),
									},
								},
							},
						},
					},
				},
			}
		})

		It("deletes the correct keys", func() {
			Ω(keysSetsToDelete).Should(Equal(expectedKeysSetsToDelete))
		})
	})

	Context("a mixed, partially filled tree", func() {
		BeforeEach(func() {
			expectedKeysSetsToDelete = [][]string{
				{},
				{"/0/0", "/0/1"},
				{"/0/0/0", "/0/1/0"},
				{"/0/1/0/0"},
			}
			exampleTree = db.StoreNode{
				Key: "/0",
				Dir: true,
				ChildNodes: []db.StoreNode{
					{
						Key: "/0/0",
						Dir: true,
						ChildNodes: []db.StoreNode{
							{
								Key:        "/0/0/0",
								Dir:        true,
								ChildNodes: []db.StoreNode{},
							},
						},
					}, {
						Key: "/0/1",
						Dir: true,
						ChildNodes: []db.StoreNode{
							{
								Key: "/0/1/0",
								Dir: true,
								ChildNodes: []db.StoreNode{
									{
										Key:   "/0/1/0/0",
										Value: []byte("false"),
									},
								},
							},
						},
					},
					{
						Key: "/0/2",
						Dir: true,
						ChildNodes: []db.StoreNode{
							{
								Key: "/0/2/0",
								Dir: true,
								ChildNodes: []db.StoreNode{
									{
										Key:   "/0/2/0/0",
										Value: []byte("true"),
									},
								},
							},
						},
					},
				},
			}
		})

		It("deletes the correct keys", func() {
			Ω(keysSetsToDelete).Should(Equal(expectedKeysSetsToDelete))
		})
	})
})
