// Package compat carries opt-in internal API extensions without modifying upstream SDK types.
package compat

import (
	"context"
	"k8s.io/client-go/rest"
	"net/http"
	"net/url"
)

type queryKey struct{}

// WithQuery attaches internal LIST/WATCH parameters to a single operation.
func WithQuery(ctx context.Context, values url.Values) context.Context {
	copied := url.Values{}
	if old, ok := ctx.Value(queryKey{}).(url.Values); ok {
		for k, v := range old {
			copied[k] = append([]string(nil), v...)
		}
	}
	for k, v := range values {
		copied[k] = append([]string(nil), v...)
	}
	return context.WithValue(ctx, queryKey{}, copied)
}

// Query returns a copy of the request-local internal parameters.
func Query(ctx context.Context) url.Values {
	out := url.Values{}
	if values, ok := ctx.Value(queryKey{}).(url.Values); ok {
		for k, v := range values {
			out[k] = append([]string(nil), v...)
		}
	}
	return out
}

type queryTransport struct{ next http.RoundTripper }

func (t queryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	values, ok := req.Context().Value(queryKey{}).(url.Values)
	if !ok || req.Method != http.MethodGet {
		return t.next.RoundTrip(req)
	}
	clone := req.Clone(req.Context())
	u := *req.URL
	clone.URL = &u
	query := u.Query()
	for k, v := range values {
		query[k] = append([]string(nil), v...)
	}
	clone.URL.RawQuery = query.Encode()
	return t.next.RoundTrip(clone)
}

// WrapConfig must be applied before typed or metadata clients are constructed.
func WrapConfig(cfg *rest.Config) *rest.Config {
	cfg = rest.CopyConfig(cfg)
	cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper { return queryTransport{next: rt} })
	return cfg
}
