package lrp_bbs

import (
	"path"
	"time"

	"github.com/cloudfoundry-incubator/runtime-schema/bbs/shared"
	"github.com/cloudfoundry/storeadapter"
)

func (bbs *LRPBBS) BumpFreshness(domain string, ttl time.Duration) error {
	return bbs.store.SetMulti([]storeadapter.StoreNode{
		{
			Key: shared.FreshnessSchemaPath(domain),
			TTL: uint64(ttl.Seconds()),
		},
	})
}

func (bbs *LRPBBS) CheckFreshness(domain string) error {
	_, err := bbs.store.Get(shared.FreshnessSchemaPath(domain))
	return err
}

func (bbs *LRPBBS) GetAllFreshness() ([]string, error) {
	node, err := bbs.store.ListRecursively(shared.FreshnessSchemaRoot)
	if err != nil && err != storeadapter.ErrorKeyNotFound {
		return nil, err
	}

	var domains []string
	for _, node := range node.ChildNodes {
		domains = append(domains, path.Base(node.Key))
	}

	return domains, nil
}
