package main

import (
	"flag"

	executorinit "github.com/cloudfoundry-incubator/executor/initializer"
)

var gardenNetwork = flag.String(
	"gardenNetwork",
	executorinit.DefaultConfiguration.GardenNetwork,
	"network mode for garden server (tcp, unix)",
)

var gardenAddr = flag.String(
	"gardenAddr",
	executorinit.DefaultConfiguration.GardenAddr,
	"network address for garden server",
)

var memoryMBFlag = flag.String(
	"memoryMB",
	executorinit.DefaultConfiguration.MemoryMB,
	"the amount of memory the executor has available in megabytes",
)

var diskMBFlag = flag.String(
	"diskMB",
	executorinit.DefaultConfiguration.DiskMB,
	"the amount of disk the executor has available in megabytes",
)

var tempDir = flag.String(
	"tempDir",
	executorinit.DefaultConfiguration.TempDir,
	"location to store temporary assets",
)

var registryPruningInterval = flag.Duration(
	"pruneInterval",
	executorinit.DefaultConfiguration.RegistryPruningInterval,
	"amount of time during which a container can remain in the allocated state",
)

var containerInodeLimit = flag.Uint64(
	"containerInodeLimit",
	executorinit.DefaultConfiguration.ContainerInodeLimit,
	"max number of inodes per container",
)

var containerMaxCpuShares = flag.Uint64(
	"containerMaxCpuShares",
	executorinit.DefaultConfiguration.ContainerMaxCpuShares,
	"cpu shares allocatable to a container",
)

var cachePath = flag.String(
	"cachePath",
	executorinit.DefaultConfiguration.CachePath,
	"location to cache assets",
)

var maxCacheSizeInBytes = flag.Uint64(
	"maxCacheSizeInBytes",
	executorinit.DefaultConfiguration.MaxCacheSizeInBytes,
	"maximum size of the cache (in bytes) - you should include a healthy amount of overhead",
)

var skipCertVerify = flag.Bool(
	"skipCertVerify",
	executorinit.DefaultConfiguration.SkipCertVerify,
	"skip SSL certificate verification",
)

var healthyMonitoringInterval = flag.Duration(
	"healthyMonitoringInterval",
	executorinit.DefaultConfiguration.HealthyMonitoringInterval,
	"interval on which to check healthy containers",
)

var unhealthyMonitoringInterval = flag.Duration(
	"unhealthyMonitoringInterval",
	executorinit.DefaultConfiguration.UnhealthyMonitoringInterval,
	"interval on which to check unhealthy containers",
)

var exportNetworkEnvVars = flag.Bool(
	"exportNetworkEnvVars",
	executorinit.DefaultConfiguration.ExportNetworkEnvVars,
	"export network environment variables into container (e.g. CF_INSTANCE_IP, CF_INSTANCE_PORT)",
)

var containerOwnerName = flag.String(
	"containerOwnerName",
	executorinit.DefaultConfiguration.ContainerOwnerName,
	"owner name with which to tag containers",
)

var createWorkPoolSize = flag.Int(
	"createWorkPoolSize",
	executorinit.DefaultConfiguration.CreateWorkPoolSize,
	"Number of concurrent create operations in garden",
)

var deleteWorkPoolSize = flag.Int(
	"deleteWorkPoolSize",
	executorinit.DefaultConfiguration.DeleteWorkPoolSize,
	"Number of concurrent delete operations in garden",
)

var readWorkPoolSize = flag.Int(
	"readWorkPoolSize",
	executorinit.DefaultConfiguration.ReadWorkPoolSize,
	"Number of concurrent read operations in garden",
)

var metricsWorkPoolSize = flag.Int(
	"metricsWorkPoolSize",
	executorinit.DefaultConfiguration.MetricsWorkPoolSize,
	"Number of concurrent metrics operations in garden",
)

var healthCheckWorkPoolSize = flag.Int(
	"healthCheckWorkPoolSize",
	executorinit.DefaultConfiguration.HealthCheckWorkPoolSize,
	"Number of concurrent ping operations in garden",
)

var maxConcurrentDownloads = flag.Int(
	"maxConcurrentDownloads",
	executorinit.DefaultConfiguration.MaxConcurrentDownloads,
	"Number of concurrent download steps",
)

var gardenHealthcheckInterval = flag.Duration(
	"gardenHealthcheckInterval",
	executorinit.DefaultConfiguration.GardenHealthcheckInterval,
	"Frequency for healthchecking garden",
)

var gardenHealthcheckTimeout = flag.Duration(
	"gardenHealthcheckTimeout",
	executorinit.DefaultConfiguration.GardenHealthcheckTimeout,
	"Maximum allowed time for garden healthcheck",
)

var gardenHealthcheckRetryCommandInterval = flag.Duration(
	"gardenHealthcheckRetryCommandInterval",
	executorinit.DefaultConfiguration.GardenHealthcheckCommandTimeout,
	"Time to wait between retrying garden commands",
)

func executorConfig() executorinit.Configuration {
	return executorinit.Configuration{
		GardenNetwork:                   *gardenNetwork,
		GardenAddr:                      *gardenAddr,
		ContainerOwnerName:              *containerOwnerName,
		TempDir:                         *tempDir,
		CachePath:                       *cachePath,
		MaxCacheSizeInBytes:             *maxCacheSizeInBytes,
		SkipCertVerify:                  *skipCertVerify,
		ExportNetworkEnvVars:            *exportNetworkEnvVars,
		ContainerMaxCpuShares:           *containerMaxCpuShares,
		ContainerInodeLimit:             *containerInodeLimit,
		HealthyMonitoringInterval:       *healthyMonitoringInterval,
		UnhealthyMonitoringInterval:     *unhealthyMonitoringInterval,
		HealthCheckWorkPoolSize:         *healthCheckWorkPoolSize,
		CreateWorkPoolSize:              *createWorkPoolSize,
		DeleteWorkPoolSize:              *deleteWorkPoolSize,
		ReadWorkPoolSize:                *readWorkPoolSize,
		MetricsWorkPoolSize:             *metricsWorkPoolSize,
		RegistryPruningInterval:         *registryPruningInterval,
		MemoryMB:                        *memoryMBFlag,
		DiskMB:                          *diskMBFlag,
		MaxConcurrentDownloads:          *maxConcurrentDownloads,
		GardenHealthcheckInterval:       *gardenHealthcheckInterval,
		GardenHealthcheckTimeout:        *gardenHealthcheckTimeout,
		GardenHealthcheckCommandTimeout: *gardenHealthcheckRetryCommandInterval,
	}
}
