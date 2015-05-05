package main

import (
	"flag"

	ExecutorInitializer "github.com/cloudfoundry-incubator/executor/initializer"
)

var gardenNetwork = flag.String(
	"gardenNetwork",
	ExecutorInitializer.DefaultConfiguration.GardenNetwork,
	"network mode for garden server (tcp, unix)",
)

var gardenAddr = flag.String(
	"gardenAddr",
	ExecutorInitializer.DefaultConfiguration.GardenAddr,
	"network address for garden server",
)

var memoryMBFlag = flag.String(
	"memoryMB",
	ExecutorInitializer.DefaultConfiguration.MemoryMB,
	"the amount of memory the executor has available in megabytes",
)

var diskMBFlag = flag.String(
	"diskMB",
	ExecutorInitializer.DefaultConfiguration.DiskMB,
	"the amount of disk the executor has available in megabytes",
)

var tempDir = flag.String(
	"tempDir",
	ExecutorInitializer.DefaultConfiguration.TempDir,
	"location to store temporary assets",
)

var registryPruningInterval = flag.Duration(
	"pruneInterval",
	ExecutorInitializer.DefaultConfiguration.RegistryPruningInterval,
	"amount of time during which a container can remain in the allocated state",
)

var containerInodeLimit = flag.Uint64(
	"containerInodeLimit",
	ExecutorInitializer.DefaultConfiguration.ContainerInodeLimit,
	"max number of inodes per container",
)

var containerMaxCpuShares = flag.Uint64(
	"containerMaxCpuShares",
	ExecutorInitializer.DefaultConfiguration.ContainerMaxCpuShares,
	"cpu shares allocatable to a container",
)

var cachePath = flag.String(
	"cachePath",
	ExecutorInitializer.DefaultConfiguration.CachePath,
	"location to cache assets",
)

var maxCacheSizeInBytes = flag.Uint64(
	"maxCacheSizeInBytes",
	ExecutorInitializer.DefaultConfiguration.MaxCacheSizeInBytes,
	"maximum size of the cache (in bytes) - you should include a healthy amount of overhead",
)

var allowPrivileged = flag.Bool(
	"allowPrivileged",
	ExecutorInitializer.DefaultConfiguration.AllowPrivileged,
	"allow execution of privileged run actions",
)

var skipCertVerify = flag.Bool(
	"skipCertVerify",
	ExecutorInitializer.DefaultConfiguration.SkipCertVerify,
	"skip SSL certificate verification",
)

var healthyMonitoringInterval = flag.Duration(
	"healthyMonitoringInterval",
	ExecutorInitializer.DefaultConfiguration.HealthyMonitoringInterval,
	"interval on which to check healthy containers",
)

var unhealthyMonitoringInterval = flag.Duration(
	"unhealthyMonitoringInterval",
	ExecutorInitializer.DefaultConfiguration.UnhealthyMonitoringInterval,
	"interval on which to check unhealthy containers",
)

var exportNetworkEnvVars = flag.Bool(
	"exportNetworkEnvVars",
	ExecutorInitializer.DefaultConfiguration.ExportNetworkEnvVars,
	"export network environment variables into container (e.g. CF_INSTANCE_IP, CF_INSTANCE_PORT)",
)

var containerOwnerName = flag.String(
	"containerOwnerName",
	ExecutorInitializer.DefaultConfiguration.ContainerOwnerName,
	"owner name with which to tag containers",
)

var createWorkPoolSize = flag.Int(
	"createWorkPoolSize",
	ExecutorInitializer.DefaultConfiguration.CreateWorkPoolSize,
	"Number of concurrent create operations in garden",
)

var deleteWorkPoolSize = flag.Int(
	"deleteWorkPoolSize",
	ExecutorInitializer.DefaultConfiguration.DeleteWorkPoolSize,
	"Number of concurrent delete operations in garden",
)

var readWorkPoolSize = flag.Int(
	"readWorkPoolSize",
	ExecutorInitializer.DefaultConfiguration.ReadWorkPoolSize,
	"Number of concurrent read operations in garden",
)

var metricsWorkPoolSize = flag.Int(
	"metricsWorkPoolSize",
	ExecutorInitializer.DefaultConfiguration.MetricsWorkPoolSize,
	"Number of concurrent metrics operations in garden",
)

var healthCheckWorkPoolSize = flag.Int(
	"healthCheckWorkPoolSize",
	ExecutorInitializer.DefaultConfiguration.HealthCheckWorkPoolSize,
	"Number of concurrent ping operations in garden",
)

func executorConfig() ExecutorInitializer.Configuration {
	return ExecutorInitializer.Configuration{
		GardenNetwork:               *gardenNetwork,
		GardenAddr:                  *gardenAddr,
		ContainerOwnerName:          *containerOwnerName,
		TempDir:                     *tempDir,
		CachePath:                   *cachePath,
		MaxCacheSizeInBytes:         *maxCacheSizeInBytes,
		AllowPrivileged:             *allowPrivileged,
		SkipCertVerify:              *skipCertVerify,
		ExportNetworkEnvVars:        *exportNetworkEnvVars,
		ContainerMaxCpuShares:       *containerMaxCpuShares,
		ContainerInodeLimit:         *containerInodeLimit,
		HealthyMonitoringInterval:   *healthyMonitoringInterval,
		UnhealthyMonitoringInterval: *unhealthyMonitoringInterval,
		HealthCheckWorkPoolSize:     *healthCheckWorkPoolSize,
		CreateWorkPoolSize:          *createWorkPoolSize,
		DeleteWorkPoolSize:          *deleteWorkPoolSize,
		ReadWorkPoolSize:            *readWorkPoolSize,
		MetricsWorkPoolSize:         *metricsWorkPoolSize,
		RegistryPruningInterval:     *registryPruningInterval,
		MemoryMB:                    *memoryMBFlag,
		DiskMB:                      *diskMBFlag,
	}
}
