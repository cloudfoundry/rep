package rep

// All RootFS provider types have moved to code.cloudfoundry.org/bbs/models.
// These aliases maintain backward compatibility.

import bbsmodels "code.cloudfoundry.org/bbs/models"

type RootFSProvider     = bbsmodels.RootFSProvider
type RootFSProviderType = bbsmodels.RootFSProviderType
type RootFSProviders    = bbsmodels.RootFSProviders

const (
	RootFSProviderTypeArbitrary = bbsmodels.RootFSProviderTypeArbitrary
	RootFSProviderTypeFixedSet  = bbsmodels.RootFSProviderTypeFixedSet
)

type ArbitraryRootFSProvider = bbsmodels.ArbitraryRootFSProvider
type FixedSetRootFSProvider  = bbsmodels.FixedSetRootFSProvider
type StringSet               = bbsmodels.StringSet

var NewFixedSetRootFSProvider = bbsmodels.NewFixedSetRootFSProvider
var NewStringSet              = bbsmodels.NewStringSet
