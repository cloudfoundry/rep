package main

import (
	"path/filepath"

	executorinit "code.cloudfoundry.org/executor/initializer"
	"code.cloudfoundry.org/rep/cmd/rep/config"
)

func executorConfig(executorConfig *config.ExecutorConfig, caCertsForDownloads []byte, gardenHealthcheckRootFS string) executorinit.Configuration {
	return executorinit.Configuration{
		GardenNetwork:                      executorConfig.GardenNetwork,
		GardenAddr:                         executorConfig.GardenAddr,
		ContainerOwnerName:                 executorConfig.ContainerOwnerName,
		TempDir:                            executorConfig.TempDir,
		CachePath:                          executorConfig.CachePath,
		MaxCacheSizeInBytes:                executorConfig.MaxCacheSizeInBytes,
		SkipCertVerify:                     executorConfig.SkipCertVerify,
		CACertsForDownloads:                caCertsForDownloads,
		ExportNetworkEnvVars:               executorConfig.ExportNetworkEnvVars,
		ContainerMaxCpuShares:              executorConfig.ContainerMaxCpuShares,
		ContainerInodeLimit:                executorConfig.ContainerInodeLimit,
		HealthyMonitoringInterval:          executorinit.Duration(executorConfig.HealthyMonitoringInterval),
		UnhealthyMonitoringInterval:        executorinit.Duration(executorConfig.UnhealthyMonitoringInterval),
		HealthCheckWorkPoolSize:            executorConfig.HealthCheckWorkPoolSize,
		CreateWorkPoolSize:                 executorConfig.CreateWorkPoolSize,
		DeleteWorkPoolSize:                 executorConfig.DeleteWorkPoolSize,
		ReadWorkPoolSize:                   executorConfig.ReadWorkPoolSize,
		MetricsWorkPoolSize:                executorConfig.MetricsWorkPoolSize,
		ReservedExpirationTime:             executorinit.Duration(executorConfig.ReservedExpirationTime),
		ContainerReapInterval:              executorinit.Duration(executorConfig.ContainerReapInterval),
		MemoryMB:                           executorConfig.MemoryMB,
		DiskMB:                             executorConfig.DiskMB,
		MaxConcurrentDownloads:             executorConfig.MaxConcurrentDownloads,
		GardenHealthcheckInterval:          executorinit.Duration(executorConfig.GardenHealthcheckInterval),
		GardenHealthcheckEmissionInterval:  executorinit.Duration(executorConfig.GardenHealthcheckEmissionInterval),
		GardenHealthcheckTimeout:           executorinit.Duration(executorConfig.GardenHealthcheckTimeout),
		GardenHealthcheckCommandRetryPause: executorinit.Duration(executorConfig.GardenHealthcheckCommandRetryPause),
		GardenHealthcheckRootFS:            gardenHealthcheckRootFS,
		GardenHealthcheckProcessPath:       executorConfig.GardenHealthcheckProcessPath,
		GardenHealthcheckProcessUser:       executorConfig.GardenHealthcheckProcessUser,
		GardenHealthcheckProcessDir:        executorConfig.GardenHealthcheckProcessDir,
		GardenHealthcheckProcessArgs:       executorConfig.GardenHealthcheckProcessArgs,
		GardenHealthcheckProcessEnv:        executorConfig.GardenHealthcheckProcessEnv,
		PostSetupHook:                      executorConfig.PostSetupHook,
		PostSetupUser:                      executorConfig.PostSetupUser,
		TrustedSystemCertificatesPath:      executorConfig.TrustedSystemCertificatesPath,
		VolmanDriverPaths:                  filepath.SplitList(executorConfig.VolmanDriverPaths),
		ContainerMetricsReportInterval:     executorinit.Duration(executorConfig.ContainerMetricsReportInterval),
	}
}
