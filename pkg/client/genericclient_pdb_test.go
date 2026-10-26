package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func TestGenericClientPDBDiscoveryIsLazy(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/policy/v1":
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(&metav1.Status{Status: metav1.StatusFailure, Reason: metav1.StatusReasonNotFound, Code: 404})
		case "/apis/policy/v1beta1":
			_ = json.NewEncoder(w).Encode(&metav1.APIResourceList{GroupVersion: "policy/v1beta1", APIResources: []metav1.APIResource{{Name: "poddisruptionbudgets", Namespaced: true, Kind: "PodDisruptionBudget", Verbs: []string{"list", "watch"}}}})
		case "/apis/policy/v1beta1/namespaces/ns/poddisruptionbudgets":
			_, _ = w.Write([]byte(`{"apiVersion":"policy/v1beta1","kind":"PodDisruptionBudgetList","metadata":{"resourceVersion":"42"},"items":[{"metadata":{"name":"empty-beta","namespace":"ns"},"spec":{"selector":{}},"status":{"disruptionsAllowed":0,"currentHealthy":0,"desiredHealthy":0,"expectedPods":0}}]}`))
		default:
			t.Errorf("unexpected PDB route: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	generic, err := newForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 0 {
		t.Fatal("generic client initialization eagerly queried PDBs")
	}
	pdbs, err := generic.PodDisruptionBudgets("ns")
	if err != nil {
		t.Fatal(err)
	}
	list, err := pdbs.List(context.Background(), metav1.ListOptions{ResourceVersion: "42", ResourceVersionMatch: metav1.ResourceVersionMatchExact})
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 3 || list.ResourceVersion != "42" || len(list.Items) != 1 || list.Items[0].Spec.Selector != nil {
		t.Fatalf("generic client PDB compatibility failed: requests=%d list=%#v", requests.Load(), list)
	}
}
