package benchmark

import (
	"fmt"
	"regexp"
	"strings"

	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

var raySessionIDPattern = regexp.MustCompile(`^session_[A-Za-z0-9][A-Za-z0-9_.-]*$`)

type hsSourceTarget struct {
	namespace string
	cluster   string
	session   string
}

// validateHSSourceIdentity rejects values that path.Join could normalize into
// another source. Namespace and cluster use their Kubernetes object-name
// contracts; Ray session IDs are one safe path segment beginning with session_.
func validateHSSourceIdentity(namespace, cluster, session string) error {
	if errors := k8svalidation.IsDNS1123Label(namespace); len(errors) != 0 {
		return fmt.Errorf("namespace %q is not a Kubernetes DNS-1123 label: %s", namespace, strings.Join(errors, "; "))
	}
	if errors := k8svalidation.IsDNS1123Subdomain(cluster); len(errors) != 0 {
		return fmt.Errorf("cluster %q is not a Kubernetes DNS-1123 subdomain: %s", cluster, strings.Join(errors, "; "))
	}
	if len(session) > 255 || !raySessionIDPattern.MatchString(session) {
		return fmt.Errorf("session %q must be one safe Ray session_ path segment", session)
	}
	return nil
}

func parseHSSourceSpec(spec string) (hsSourceTarget, error) {
	parts := strings.Split(spec, "/")
	if len(parts) != 3 {
		return hsSourceTarget{}, fmt.Errorf("source %q must be namespace/cluster/session", spec)
	}
	if err := validateHSSourceIdentity(parts[0], parts[1], parts[2]); err != nil {
		return hsSourceTarget{}, fmt.Errorf("source %q is invalid: %w", spec, err)
	}
	return hsSourceTarget{namespace: parts[0], cluster: parts[1], session: parts[2]}, nil
}
