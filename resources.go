package rep

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	bbsmodels "code.cloudfoundry.org/bbs/models"
	"code.cloudfoundry.org/executor/containermetrics"
)

// StackPathMap maps aliases to rootFS paths on the system.
type StackPathMap map[string]string

// ErrPreloadedRootFSNotFound is returned when the given hostname of the
// rootFS could not be resolved if the scheme is the PreloadedRootFSScheme
// or the PreloadedOCIRootFSScheme.
var ErrPreloadedRootFSNotFound = fmt.Errorf("preloaded rootfs path not found")

// PathForRootFS resolves the hostname portion of the RootFS URL to the actual
// path to the preloaded rootFS on the system according to the StackPathMap.
func (m StackPathMap) PathForRootFS(rootFS string) (string, error) {
	if rootFS == "" {
		return rootFS, nil
	}

	u, err := url.Parse(rootFS)
	if err != nil {
		return "", err
	}

	if u.Scheme == bbsmodels.PreloadedRootFSScheme {
		path, ok := m[u.Opaque]
		if !ok {
			return "", ErrPreloadedRootFSNotFound
		}
		return path, nil
	} else if u.Scheme == bbsmodels.PreloadedOCIRootFSScheme {
		path, ok := m[u.Opaque]
		if !ok {
			return "", ErrPreloadedRootFSNotFound
		}
		return fmt.Sprintf("%s:%s?%s", u.Scheme, path, u.RawQuery), nil
	}

	return rootFS, nil
}

func (m StackPathMap) StackVersionList() []string {
	stackVersions := []string{}
	for name, path := range m {
		version := loadVersionFromPath(path)
		if version != "" {
			stackVersions = append(stackVersions, fmt.Sprintf("%s@%s", name, version))
		} else {
			stackVersions = append(stackVersions, name)
		}
	}
	return stackVersions
}

//go:generate counterfeiter -o auctioncellrep/auctioncellrepfakes/fake_container_metrics_provider.go . ContainerMetricsProvider
type ContainerMetricsProvider interface {
	Metrics() map[string]*containermetrics.CachedContainerMetrics
}

type ContainerMetricsCollection struct {
	CellID string       `json:"cell_id"`
	LRPs   []LRPMetric  `json:"lrps"`
	Tasks  []TaskMetric `json:"tasks"`
}

type LRPMetric struct {
	InstanceGUID string `json:"instance_guid"`
	ProcessGUID  string `json:"process_guid"`
	Index        int32  `json:"index"`
	containermetrics.CachedContainerMetrics
}

type TaskMetric struct {
	TaskGUID string `json:"task_guid"`
	containermetrics.CachedContainerMetrics
}

func loadVersionFromPath(path string) string {
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()

	tarReader := tar.NewReader(file)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}

		if header == nil {
			continue
		}

		if strings.TrimPrefix(header.Name, ".") == StackVersionFile {
			buf := new(bytes.Buffer)
			if _, err := io.Copy(buf, tarReader); err != nil {
				return ""
			}
			return strings.TrimSpace(buf.String())
		}
	}

	return ""
}
