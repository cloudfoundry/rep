package rep_test

import (
	"strconv"

	"code.cloudfoundry.org/bbs/models"
	"code.cloudfoundry.org/bbs/models/test/model_helpers"
	"code.cloudfoundry.org/bbs/test_helpers"
	"code.cloudfoundry.org/executor"
	"code.cloudfoundry.org/rep"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Resources", func() {
	Describe("ActualLRPKeyFromTags", func() {
		var (
			tags             executor.Tags
			lrpKey           *models.ActualLRPKey
			keyConversionErr error
		)

		BeforeEach(func() {
			tags = executor.Tags{
				rep.LifecycleTag:    rep.LRPLifecycle,
				rep.DomainTag:       "my-domain",
				rep.ProcessGuidTag:  "process-guid",
				rep.ProcessIndexTag: "999",
			}
		})

		JustBeforeEach(func() {
			lrpKey, keyConversionErr = rep.ActualLRPKeyFromTags(tags)
		})

		Context("when the tags are valid", func() {
			It("does not return an error", func() {
				Expect(keyConversionErr).NotTo(HaveOccurred())
			})

			It("converts a valid tags without error", func() {
				expectedKey := models.ActualLRPKey{
					ProcessGuid: "process-guid",
					Index:       999,
					Domain:      "my-domain",
				}
				Expect(*lrpKey).To(Equal(expectedKey))
			})
		})

		Context("when the tags are invalid", func() {
			Context("when the tags have no tags", func() {
				BeforeEach(func() {
					tags = nil
				})

				It("reports an error that the tags are missing", func() {
					Expect(keyConversionErr).To(MatchError(rep.ErrContainerMissingTags))
				})
			})

			Context("when the tags are missing the process guid tag ", func() {
				BeforeEach(func() {
					delete(tags, rep.ProcessGuidTag)
				})

				It("reports the process_guid is invalid", func() {
					Expect(keyConversionErr).To(HaveOccurred())
					Expect(keyConversionErr.Error()).To(ContainSubstring("process_guid"))
				})
			})

			Context("when the tags process index tag is not a number", func() {
				BeforeEach(func() {
					tags[rep.ProcessIndexTag] = "hi there"
				})

				It("reports the index is invalid when constructing ActualLRPKey", func() {
					Expect(keyConversionErr).To(MatchError(rep.ErrInvalidProcessIndex))
				})
			})
		})
	})

	Describe("ActualLRPInstanceKeyFromContainer", func() {
		var (
			container                executor.Container
			lrpInstanceKey           *models.ActualLRPInstanceKey
			instanceKeyConversionErr error
			cellID                   string
		)

		BeforeEach(func() {
			container = executor.Container{
				Guid: "container-guid",
				Tags: executor.Tags{
					rep.LifecycleTag:    rep.LRPLifecycle,
					rep.DomainTag:       "my-domain",
					rep.ProcessGuidTag:  "process-guid",
					rep.ProcessIndexTag: "999",
					rep.InstanceGuidTag: "some-instance-guid",
				},
				RunInfo: executor.RunInfo{
					Ports: []executor.PortMapping{
						{
							ContainerPort: 1234,
							HostPort:      6789,
						},
					},
				},
			}
			cellID = "the-cell-id"
		})

		JustBeforeEach(func() {
			lrpInstanceKey, instanceKeyConversionErr = rep.ActualLRPInstanceKeyFromContainer(container, cellID)
		})

		Context("when the container and cell id are valid", func() {
			It("it does not return an error", func() {
				Expect(instanceKeyConversionErr).NotTo(HaveOccurred())
			})

			It("it creates the correct container key", func() {
				expectedInstanceKey := models.ActualLRPInstanceKey{
					InstanceGuid: "some-instance-guid",
					CellId:       cellID,
				}

				Expect(*lrpInstanceKey).To(Equal(expectedInstanceKey))
			})
		})

		Context("when the container is invalid", func() {
			Context("when the container has no tags", func() {
				BeforeEach(func() {
					container.Tags = nil
				})

				It("reports an error that the tags are missing", func() {
					Expect(instanceKeyConversionErr).To(MatchError(rep.ErrContainerMissingTags))
				})
			})

			Context("when the container is missing the instance guid tag ", func() {
				BeforeEach(func() {
					delete(container.Tags, rep.InstanceGuidTag)
				})

				It("returns an invalid instance-guid error", func() {
					Expect(instanceKeyConversionErr.Error()).To(ContainSubstring("instance_guid"))
				})
			})

			Context("when the cell id is invalid", func() {
				BeforeEach(func() {
					cellID = ""
				})

				It("returns an invalid cell id error", func() {
					Expect(instanceKeyConversionErr.Error()).To(ContainSubstring("cell_id"))
				})
			})
		})
	})

	Describe("ActualLRPNetInfoFromContainer", func() {
		var (
			container            executor.Container
			lrpNetInfo           *models.ActualLRPNetInfo
			netInfoConversionErr error
		)

		BeforeEach(func() {
			container = executor.Container{
				Guid:       "some-instance-guid",
				ExternalIP: "some-external-ip",
				InternalIP: "container-ip",
				Tags: executor.Tags{
					rep.LifecycleTag:    rep.LRPLifecycle,
					rep.DomainTag:       "my-domain",
					rep.ProcessGuidTag:  "process-guid",
					rep.ProcessIndexTag: "999",
				},
				RunInfo: executor.RunInfo{
					Ports: []executor.PortMapping{
						{
							ContainerPort: 1234,
							HostPort:      6789,
						},
					},
				},
			}
		})

		JustBeforeEach(func() {
			lrpNetInfo, netInfoConversionErr = rep.ActualLRPNetInfoFromContainer(container)
		})

		Context("when container and executor host are valid", func() {
			It("does not return an error", func() {
				Expect(netInfoConversionErr).NotTo(HaveOccurred())
			})

			It("returns the correct net info", func() {
				expectedNetInfo := models.ActualLRPNetInfo{
					Ports: []*models.PortMapping{
						{
							ContainerPort: 1234,
							HostPort:      6789,
						},
					},
					Address:         "some-external-ip",
					InstanceAddress: "container-ip",
				}

				Expect(*lrpNetInfo).To(Equal(expectedNetInfo))
			})
		})

		Context("when there are no exposed ports", func() {
			BeforeEach(func() {
				container.Ports = nil
			})

			It("does not return an error", func() {
				Expect(netInfoConversionErr).NotTo(HaveOccurred())
			})
		})

		Context("when the executor host is invalid", func() {
			BeforeEach(func() {
				container.ExternalIP = ""
			})

			It("returns an invalid host error", func() {
				Expect(netInfoConversionErr.Error()).To(ContainSubstring("address"))
			})
		})
	})

	Describe("NewRunRequestFromDesiredLRP", func() {
		var (
			containerGuid string
			desiredLRP    *models.DesiredLRP
			actualLRP     *models.ActualLRP
		)

		BeforeEach(func() {
			containerGuid = "the-container-guid"
			desiredLRP = model_helpers.NewValidDesiredLRP("the-process-guid")
			desiredLRP.Ports = []uint32{8080}
			// This is a lazy way to prevent old tests from failing.  The tests
			// happily ignored ImageLayer that used to be returned from
			// NewValidDesiredLRP, but now we are converting to V2 they are
			// failing because they are getting extra CachedDependencies.  We
			// test explicitly for V2 conversion in a context below
			desiredLRP.ImageLayers = nil
			actualLRP = model_helpers.NewValidActualLRP("the-process-guid", 9)
			desiredLRP.RootFs = "preloaded://foobar"
		})

		It("returns a valid run request", func() {
			runReq, err := rep.NewRunRequestFromDesiredLRP(containerGuid, desiredLRP, &actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey)
			Expect(err).NotTo(HaveOccurred())
			Expect(runReq.Tags).To(Equal(executor.Tags{}))
			Expect(runReq.RunInfo).To(test_helpers.DeepEqual(executor.RunInfo{
				CPUWeight: uint(desiredLRP.CpuWeight),
				DiskScope: executor.ExclusiveDiskLimit,
				Ports:     rep.ConvertPortMappings(desiredLRP.Ports),
				LogConfig: executor.LogConfig{
					Guid:       desiredLRP.LogGuid,
					Index:      int(actualLRP.Index),
					SourceName: desiredLRP.LogSource,
				},
				MetricsConfig: executor.MetricsConfig{
					Guid:  desiredLRP.MetricsGuid,
					Index: int(actualLRP.Index),
				},
				StartTimeoutMs: uint(desiredLRP.StartTimeoutMs),
				Privileged:     desiredLRP.Privileged,
				CachedDependencies: []executor.CachedDependency{
					{Name: "app bits", From: "blobstore.com/bits/app-bits", To: "/usr/local/app", CacheKey: "cache-key", LogSource: "log-source"},
					{Name: "app bits with checksum", From: "blobstore.com/bits/app-bits-checksum", To: "/usr/local/app-checksum", CacheKey: "cache-key", LogSource: "log-source", ChecksumAlgorithm: "md5", ChecksumValue: "checksum-value"},
				},
				Setup:           desiredLRP.Setup,
				Action:          desiredLRP.Action,
				Monitor:         desiredLRP.Monitor,
				CheckDefinition: desiredLRP.CheckDefinition,
				EgressRules:     desiredLRP.EgressRules,
				Env: append([]executor.EnvironmentVariable{
					{Name: "INSTANCE_GUID", Value: actualLRP.InstanceGuid},
					{Name: "INSTANCE_INDEX", Value: strconv.Itoa(int(actualLRP.Index))},
					{Name: "CF_INSTANCE_GUID", Value: actualLRP.InstanceGuid},
					{Name: "CF_INSTANCE_INDEX", Value: strconv.Itoa(int(actualLRP.Index))},
				}, executor.EnvironmentVariablesFromModel(desiredLRP.EnvironmentVariables)...),
				TrustedSystemCertificatesPath: "/etc/somepath",
				VolumeMounts: []executor.VolumeMount{
					{
						Driver:        "my-driver",
						VolumeId:      "my-volume",
						ContainerPath: "/mnt/mypath",
						Config:        map[string]interface{}{"foo": "bar"},
						Mode:          executor.BindMountModeRO,
					},
				},
				Network: &executor.Network{
					Properties: map[string]string{
						"some-key":       "some-value",
						"some-other-key": "some-other-value",
					},
				},
				CertificateProperties: executor.CertificateProperties{
					OrganizationalUnit: []string{"iamthelizardking", "iamthelizardqueen"},
				},
				ImageUsername:        "image-username",
				ImagePassword:        "image-password",
				EnableContainerProxy: true,
			}))
		})

		Context("when the network is nil", func() {
			BeforeEach(func() {
				desiredLRP.Network = nil
			})

			It("sets a nil network on the result", func() {
				runReq, err := rep.NewRunRequestFromDesiredLRP(containerGuid, desiredLRP, &actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(runReq.Network).To(BeNil())
			})
		})

		Context("when the certificate properties are nil", func() {
			BeforeEach(func() {
				desiredLRP.CertificateProperties = nil
			})

			It("it sets an empty certificate properties on the result", func() {
				runReq, err := rep.NewRunRequestFromDesiredLRP(containerGuid, desiredLRP, &actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(runReq.CertificateProperties).To(Equal(executor.CertificateProperties{}))
			})
		})

		Context("when a volumeMount config is invalid", func() {
			BeforeEach(func() {
				desiredLRP.VolumeMounts[0].Shared.MountConfig = "{{"
			})

			It("returns an error", func() {
				_, err := rep.NewRunRequestFromDesiredLRP(containerGuid, desiredLRP, &actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey)
				Expect(err).To(HaveOccurred())
			})
		})

		It("enables the envoy proxy", func() {
			runReq, err := rep.NewRunRequestFromDesiredLRP(containerGuid, desiredLRP, &actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey)
			Expect(err).NotTo(HaveOccurred())
			Expect(runReq.EnableContainerProxy).To(BeTrue())
		})

		Context("when the LRP doesn't have any exposed ports", func() {
			BeforeEach(func() {
				desiredLRP.Ports = nil
			})

			It("disables the envoy proxy", func() {
				runReq, err := rep.NewRunRequestFromDesiredLRP(containerGuid, desiredLRP, &actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(runReq.EnableContainerProxy).To(BeFalse())
			})
		})

		Context("when the rootfs is preloaded+layer", func() {
			BeforeEach(func() {
				desiredLRP.RootFs = "preloaded+layer:cflinuxfs2?layer=http://file-server/layer.tgz&layer_digest=some-digest&layer_path=/path/in/container"
			})

			It("enables the envoy proxy", func() {
				runReq, err := rep.NewRunRequestFromDesiredLRP(containerGuid, desiredLRP, &actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(runReq.EnableContainerProxy).To(BeTrue())
			})

			It("uses ExclusiveDiskLimit as the disk scope", func() {
				runReq, err := rep.NewRunRequestFromDesiredLRP(containerGuid, desiredLRP, &actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(runReq.DiskScope).To(Equal(executor.ExclusiveDiskLimit))
			})
		})

		Context("when the rootfs is not preloaded", func() {
			BeforeEach(func() {
				desiredLRP.RootFs = "docker://cloudfoundry/test"
			})

			It("enables the envoy proxy", func() {
				runReq, err := rep.NewRunRequestFromDesiredLRP(containerGuid, desiredLRP, &actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(runReq.EnableContainerProxy).To(BeTrue())
			})

			It("uses TotalDiskLimit as the disk scope", func() {
				runReq, err := rep.NewRunRequestFromDesiredLRP(containerGuid, desiredLRP, &actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(runReq.DiskScope).To(Equal(executor.TotalDiskLimit))
			})
		})

		Context("when the lrp has V3 declarative Resources", func() {
			var (
				origSetup *models.Action
			)

			BeforeEach(func() {
				desiredLRP.CachedDependencies = nil
				origSetup = desiredLRP.Setup
				desiredLRP.ImageLayers = []*models.ImageLayer{
					{
						Name:            "app bits",
						Url:             "blobstore.com/bits/app-bits",
						DestinationPath: "/usr/local/app",
						LayerType:       models.LayerTypeShared,
						MediaType:       models.MediaTypeTgz,
						DigestAlgorithm: models.DigestAlgorithmSha256,
						DigestValue:     "some-sha256",
					},
					{
						Name:            "other bits with checksum",
						Url:             "blobstore.com/bits/other-bits-checksum",
						DestinationPath: "/usr/local/other",
						LayerType:       models.LayerTypeExclusive,
						MediaType:       models.MediaTypeTgz,
						DigestAlgorithm: models.DigestAlgorithmSha512,
						DigestValue:     "some-sha512",
					},
				}
			})

			It("converts exclusive resources into download steps", func() {
				runReq, err := rep.NewRunRequestFromDesiredLRP(containerGuid, desiredLRP, &actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(runReq.Setup).To(Equal(models.WrapAction(models.Serial(
					models.Parallel(
						&models.DownloadAction{
							Artifact:          "other bits with checksum",
							From:              "blobstore.com/bits/other-bits-checksum",
							To:                "/usr/local/other",
							CacheKey:          "sha512:some-sha512",
							LogSource:         "",
							User:              "legacy-dan",
							ChecksumAlgorithm: "sha512",
							ChecksumValue:     "some-sha512",
						},
					),
					models.UnwrapAction(origSetup),
				))))
			})

			It("converts shared resources into V2 cached dependencies", func() {
				runReq, err := rep.NewRunRequestFromDesiredLRP(containerGuid, desiredLRP, &actualLRP.ActualLRPKey, &actualLRP.ActualLRPInstanceKey)
				Expect(err).NotTo(HaveOccurred())
				Expect(runReq.CachedDependencies).To(ConsistOf(executor.CachedDependency{
					Name:              "app bits",
					From:              "blobstore.com/bits/app-bits",
					To:                "/usr/local/app",
					CacheKey:          "sha256:some-sha256",
					LogSource:         "",
					ChecksumAlgorithm: "sha256",
					ChecksumValue:     "some-sha256",
				}))
			})
		})
	})

	Describe("NewRunRequestFromTask", func() {
		var task *models.Task
		BeforeEach(func() {
			task = model_helpers.NewValidTask("task-guid")
			// This is a lazy way to prevent old tests from failing.  The tests
			// happily ignored ImageLayer that used to be returned from
			// NewValidTask, but now we are converting to V2 they are failing
			// because they are getting extra CachedDependencies.  We test
			// explicitly for V2 conversion in a context below
			task.ImageLayers = nil
			task.RootFs = "preloaded://rootfs"
		})

		It("returns a valid run request", func() {
			runReq, err := rep.NewRunRequestFromTask(task)
			Expect(err).NotTo(HaveOccurred())
			Expect(runReq.Tags).To(Equal(executor.Tags{
				rep.ResultFileTag: task.ResultFile,
			}))

			Expect(runReq.RunInfo).To(Equal(executor.RunInfo{
				DiskScope:  executor.ExclusiveDiskLimit,
				CPUWeight:  uint(task.CpuWeight),
				Privileged: task.Privileged,
				CachedDependencies: []executor.CachedDependency{
					{Name: "app bits", From: "blobstore.com/bits/app-bits", To: "/usr/local/app", CacheKey: "cache-key", LogSource: "log-source"},
					{Name: "app bits with checksum", From: "blobstore.com/bits/app-bits-checksum", To: "/usr/local/app-checksum", CacheKey: "cache-key", LogSource: "log-source", ChecksumAlgorithm: "md5", ChecksumValue: "checksum-value"},
				},
				LogConfig: executor.LogConfig{
					Guid:       task.LogGuid,
					SourceName: task.LogSource,
				},
				MetricsConfig: executor.MetricsConfig{
					Guid: task.MetricsGuid,
				},
				Action:                        task.Action,
				Env:                           executor.EnvironmentVariablesFromModel(task.EnvironmentVariables),
				EgressRules:                   task.EgressRules,
				TrustedSystemCertificatesPath: "/etc/somepath",
				VolumeMounts: []executor.VolumeMount{{
					Driver:        "my-driver",
					VolumeId:      "my-volume",
					ContainerPath: "/mnt/mypath",
					Config:        map[string]interface{}{"foo": "bar"},
					Mode:          executor.BindMountModeRO,
				}},
				Network: &executor.Network{
					Properties: map[string]string{
						"some-key":       "some-value",
						"some-other-key": "some-other-value",
					},
				},
				CertificateProperties: executor.CertificateProperties{
					OrganizationalUnit: []string{"iamthelizardking", "iamthelizardqueen"},
				},
				ImageUsername:        "image-username",
				ImagePassword:        "image-password",
				EnableContainerProxy: false,
			}))
		})

		Context("when the network is nil", func() {
			BeforeEach(func() {
				task.Network = nil
			})

			It("sets a nil network on the result", func() {
				runReq, err := rep.NewRunRequestFromTask(task)
				Expect(err).NotTo(HaveOccurred())
				Expect(runReq.Network).To(BeNil())
			})
		})

		Context("when the certificate properties are nil", func() {
			BeforeEach(func() {
				task.CertificateProperties = nil
			})

			It("it sets an empty certificate properties on the result", func() {
				runReq, err := rep.NewRunRequestFromTask(task)
				Expect(err).NotTo(HaveOccurred())
				Expect(runReq.CertificateProperties).To(Equal(executor.CertificateProperties{}))
			})
		})

		It("disables the envoy proxy", func() {
			runReq, err := rep.NewRunRequestFromTask(task)
			Expect(err).NotTo(HaveOccurred())
			Expect(runReq.EnableContainerProxy).To(BeFalse())
		})

		Context("when the rootfs is not preloaded", func() {
			BeforeEach(func() {
				task.RootFs = "docker://cloudfoundry/test"
			})

			It("disables the envoy proxy", func() {
				runReq, err := rep.NewRunRequestFromTask(task)
				Expect(err).NotTo(HaveOccurred())
				Expect(runReq.EnableContainerProxy).To(BeFalse())
			})

			It("uses TotalDiskLimit as the disk scope", func() {
				runReq, err := rep.NewRunRequestFromTask(task)
				Expect(err).NotTo(HaveOccurred())
				Expect(runReq.DiskScope).To(Equal(executor.TotalDiskLimit))
			})
		})

		Context("when a volumeMount config is invalid", func() {
			BeforeEach(func() {
				task.VolumeMounts[0].Shared.MountConfig = "{{"
			})

			It("returns an error", func() {
				_, err := rep.NewRunRequestFromTask(task)
				Expect(err).To(MatchError("invalid character '{' looking for beginning of object key string"))
			})
		})

		Context("when the task has V3 declarative Resources", func() {
			var (
				origAction *models.Action
			)

			BeforeEach(func() {
				task.CachedDependencies = nil
				origAction = task.Action
				task.ImageLayers = []*models.ImageLayer{
					{
						Name:            "app bits",
						Url:             "blobstore.com/bits/app-bits",
						DestinationPath: "/usr/local/app",
						LayerType:       models.LayerTypeShared,
						MediaType:       models.MediaTypeTgz,
						DigestAlgorithm: models.DigestAlgorithmSha256,
						DigestValue:     "some-sha256",
					},
					{
						Name:            "other bits with checksum",
						Url:             "blobstore.com/bits/other-bits-checksum",
						DestinationPath: "/usr/local/other",
						LayerType:       models.LayerTypeExclusive,
						MediaType:       models.MediaTypeTgz,
						DigestAlgorithm: models.DigestAlgorithmSha512,
						DigestValue:     "some-sha512",
					},
				}
			})

			It("converts exclusive resources into download steps", func() {
				runReq, err := rep.NewRunRequestFromTask(task)
				Expect(err).NotTo(HaveOccurred())
				Expect(runReq.Action).To(Equal(models.WrapAction(models.Serial(
					models.Parallel(
						&models.DownloadAction{
							Artifact:          "other bits with checksum",
							From:              "blobstore.com/bits/other-bits-checksum",
							To:                "/usr/local/other",
							CacheKey:          "sha512:some-sha512",
							LogSource:         "",
							User:              "legacy-jim",
							ChecksumAlgorithm: "sha512",
							ChecksumValue:     "some-sha512",
						},
					),
					models.UnwrapAction(origAction),
				))))
			})

			It("converts shared resources into V2 cached dependencies", func() {
				runReq, err := rep.NewRunRequestFromTask(task)
				Expect(err).NotTo(HaveOccurred())
				Expect(runReq.CachedDependencies).To(ConsistOf(executor.CachedDependency{
					Name:              "app bits",
					From:              "blobstore.com/bits/app-bits",
					To:                "/usr/local/app",
					CacheKey:          "sha256:some-sha256",
					LogSource:         "",
					ChecksumAlgorithm: "sha256",
					ChecksumValue:     "some-sha256",
				}))
			})
		})
	})
})
