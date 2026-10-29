package compat

import (
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/discovery"
)

// ResourceVersion selects a served resource, never the cluster's version string.
// Authorization or transport failures must not trigger a version downgrade.
func ResourceVersion(d discovery.DiscoveryInterface, group, resource string, versions ...string) (string, error) {
	for _, version := range versions {
		gv := version
		if group != "" {
			gv = group + "/" + version
		}
		resources, err := d.ServerResourcesForGroupVersion(gv)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		for _, r := range resources.APIResources {
			if r.Name == resource {
				return version, nil
			}
		}
	}
	return "", fmt.Errorf("no served version for %s/%s among %v", group, resource, versions)
}
