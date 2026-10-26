package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	clientfeatures "k8s.io/client-go/features"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

func pdbHTTPClient(t *testing.T, handler http.HandlerFunc) kubernetes.Interface {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := kubernetes.NewForConfig(WrapConfig(&rest.Config{Host: server.URL}))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func pdbWriteJSON(w http.ResponseWriter, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func pdbDiscovery(w http.ResponseWriter, version string, verbs ...string) {
	pdbWriteJSON(w, &metav1.APIResourceList{GroupVersion: "policy/" + version, APIResources: []metav1.APIResource{{Name: "poddisruptionbudgets", Namespaced: true, Kind: "PodDisruptionBudget", Verbs: verbs}}})
}

func pdbStatus(w http.ResponseWriter, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	reasons := map[int]metav1.StatusReason{403: metav1.StatusReasonForbidden, 404: metav1.StatusReasonNotFound, 409: metav1.StatusReasonConflict, 429: metav1.StatusReasonTooManyRequests}
	_ = json.NewEncoder(w).Encode(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: reasons[code], Code: int32(code), Message: "original API error"})
}

func pdbFixtures(version string) []map[string]interface{} {
	selectors := []interface{}{nil, map[string]interface{}{}, map[string]interface{}{"matchLabels": map[string]string{"app": "web"}}, map[string]interface{}{"matchExpressions": []map[string]interface{}{{"key": "app", "operator": "In", "values": []string{"web"}}}}}
	result := make([]map[string]interface{}, len(selectors))
	for i, selector := range selectors {
		result[i] = map[string]interface{}{
			"apiVersion": "policy/" + version, "kind": "PodDisruptionBudget",
			"metadata": map[string]interface{}{"name": fmt.Sprintf("budget-%d", i), "namespace": "ns", "uid": fmt.Sprintf("uid-%d", i), "resourceVersion": "42", "generation": 7, "annotations": map[string]string{"original": "kept"}},
			"spec":     map[string]interface{}{"minAvailable": "50%", "selector": selector, "unhealthyPodEvictionPolicy": "AlwaysAllow"},
			"status":   map[string]interface{}{"observedGeneration": 7, "disruptionsAllowed": 1, "currentHealthy": 3, "desiredHealthy": 2, "expectedPods": 4, "disruptedPods": map[string]string{"old-pod": "2026-10-26T00:00:00Z"}, "conditions": []map[string]interface{}{{"type": "DisruptionAllowed", "status": "True", "observedGeneration": 7, "lastTransitionTime": "2026-10-26T00:00:00Z", "reason": "SufficientPods", "message": "status preserved"}}},
		}
	}
	return result
}

func assertPDBFieldsAndSelector(t *testing.T, pdb *policyv1.PodDisruptionBudget, index int, version string) {
	t.Helper()
	if pdb.Name != fmt.Sprintf("budget-%d", index) || pdb.Namespace != "ns" || pdb.ResourceVersion != "42" || pdb.Generation != 7 || string(pdb.UID) != fmt.Sprintf("uid-%d", index) || pdb.Annotations["original"] != "kept" {
		t.Fatalf("metadata lost: %#v", pdb.ObjectMeta)
	}
	if pdb.Spec.MinAvailable == nil || pdb.Spec.MinAvailable.String() != "50%" || pdb.Spec.UnhealthyPodEvictionPolicy == nil || *pdb.Spec.UnhealthyPodEvictionPolicy != policyv1.AlwaysAllow {
		t.Fatalf("spec lost: %#v", pdb.Spec)
	}
	status := pdb.Status
	if status.ObservedGeneration != 7 || status.DisruptionsAllowed != 1 || status.CurrentHealthy != 3 || status.DesiredHealthy != 2 || status.ExpectedPods != 4 || len(status.DisruptedPods) != 1 || status.DisruptedPods["old-pod"].Unix() != 1792972800 || len(status.Conditions) != 1 || status.Conditions[0].Message != "status preserved" || status.Conditions[0].Reason != "SufficientPods" || status.Conditions[0].ObservedGeneration != 7 || status.Conditions[0].Status != metav1.ConditionTrue {
		t.Fatalf("status lost: %#v", status)
	}
	selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	if err != nil {
		t.Fatal(err)
	}
	wantWeb := index >= 2 || (index == 1 && version == "v1")
	wantOther := index == 1 && version == "v1"
	if selector.Matches(labels.Set{"app": "web"}) != wantWeb || selector.Matches(labels.Set{"app": "other"}) != wantOther {
		t.Fatalf("selector semantics changed for %s #%d: %#v", version, index, pdb.Spec.Selector)
	}
}

func TestPDBHTTPListWatchVersionsAndSelectors(t *testing.T) {
	for _, version := range []string{"v1beta1", "v1"} {
		t.Run(version, func(t *testing.T) {
			fixtures := pdbFixtures(version)
			var mu sync.Mutex
			var paths []string
			client := pdbHTTPClient(t, func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				paths = append(paths, r.URL.Path)
				mu.Unlock()
				if r.Method != http.MethodGet {
					t.Errorf("unexpected mutation %s", r.Method)
					pdbStatus(w, 403)
					return
				}
				if r.URL.Path == "/apis/policy/v1" && version == "v1beta1" {
					pdbStatus(w, 404)
					return
				}
				if r.URL.Path == "/apis/policy/"+version {
					pdbDiscovery(w, version, "list", "watch")
					return
				}
				if r.URL.Path != "/apis/policy/"+version+"/namespaces/ns/poddisruptionbudgets" {
					t.Errorf("unexpected path %s", r.URL.Path)
					pdbStatus(w, 404)
					return
				}
				q := r.URL.Query()
				if q.Get("resourceVersion") != "42" || q.Get("labelSelector") != "team=web" || q.Get("fieldSelector") != "metadata.name=budget" {
					t.Errorf("query options lost: %v", q)
				}
				if q.Get("watch") == "true" {
					if q.Get("allowWatchBookmarks") != "true" {
						t.Error("bookmark option lost")
					}
					w.Header().Set("Content-Type", "application/json")
					for i, object := range fixtures {
						pdbWriteJSON(w, map[string]interface{}{"type": "ADDED", "object": object})
						if i == 0 {
							pdbWriteJSON(w, map[string]interface{}{"type": "MODIFIED", "object": object})
							pdbWriteJSON(w, map[string]interface{}{"type": "DELETED", "object": object})
						}
					}
					pdbWriteJSON(w, map[string]interface{}{"type": "BOOKMARK", "object": map[string]interface{}{"apiVersion": "policy/" + version, "kind": "PodDisruptionBudget", "metadata": map[string]string{"resourceVersion": "43"}}})
					pdbWriteJSON(w, map[string]interface{}{"type": "ERROR", "object": &metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Code: 410, Reason: metav1.StatusReasonExpired, Message: "original watch expiration"}})
					w.(http.Flusher).Flush()
					return
				}
				if q.Get("continue") != "page-token" || q.Get("limit") != "4" || q.Get("resourceVersionMatch") != "Exact" {
					t.Errorf("list pagination lost: %v", q)
				}
				pdbWriteJSON(w, map[string]interface{}{"apiVersion": "policy/" + version, "kind": "PodDisruptionBudgetList", "metadata": map[string]interface{}{"resourceVersion": "42", "continue": "next-page", "remainingItemCount": 8}, "items": fixtures})
			})
			pdbs, err := NewPodDisruptionBudgetClient(client, "ns")
			if err != nil {
				t.Fatal(err)
			}
			options := metav1.ListOptions{ResourceVersion: "42", ResourceVersionMatch: metav1.ResourceVersionMatchExact, LabelSelector: "team=web", FieldSelector: "metadata.name=budget", Continue: "page-token", Limit: 4}
			list, err := pdbs.List(context.Background(), options)
			if err != nil {
				t.Fatal(err)
			}
			if list.ResourceVersion != "42" || list.Continue != "next-page" || list.RemainingItemCount == nil || *list.RemainingItemCount != 8 || len(list.Items) != 4 {
				t.Fatalf("list metadata lost: %#v", list)
			}
			for i := range list.Items {
				assertPDBFieldsAndSelector(t, &list.Items[i], i, version)
			}
			options.Continue, options.Limit, options.ResourceVersionMatch = "", 0, ""
			options.AllowWatchBookmarks = true
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			stream, err := pdbs.Watch(ctx, options)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Stop()
			var types []watch.EventType
			for event := range stream.ResultChan() {
				types = append(types, event.Type)
				if event.Type == watch.Error {
					status, ok := event.Object.(*metav1.Status)
					if !ok || status.Code != 410 || status.Reason != metav1.StatusReasonExpired || status.Message != "original watch expiration" {
						t.Fatalf("error changed: %#v", event)
					}
					continue
				}
				pdb, ok := event.Object.(*policyv1.PodDisruptionBudget)
				if !ok {
					t.Fatalf("LIST/WATCH object types differ: %T", event.Object)
				}
				if event.Type == watch.Bookmark {
					if pdb.ResourceVersion != "43" {
						t.Fatal("bookmark resourceVersion lost")
					}
					continue
				}
				var index int
				_, _ = fmt.Sscanf(pdb.Name, "budget-%d", &index)
				assertPDBFieldsAndSelector(t, pdb, index, version)
			}
			wantTypes := []watch.EventType{watch.Added, watch.Modified, watch.Deleted, watch.Added, watch.Added, watch.Added, watch.Bookmark, watch.Error}
			if !reflect.DeepEqual(types, wantTypes) {
				t.Fatalf("events lost: %v", types)
			}
			mu.Lock()
			defer mu.Unlock()
			wantRequests := 3
			if version == "v1beta1" {
				wantRequests++
			}
			if len(paths) != wantRequests {
				t.Fatalf("unexpected retry/discovery: %v", paths)
			}
		})
	}
}

func TestPDBHTTPFailuresNeverDowngrade(t *testing.T) {
	checks := map[int]func(error) bool{403: apierrors.IsForbidden, 404: apierrors.IsNotFound, 409: apierrors.IsConflict, 429: apierrors.IsTooManyRequests}
	for _, operation := range []string{"discovery", "list", "watch"} {
		for _, code := range []int{403, 404, 409, 429} {
			if operation == "discovery" && code == 404 {
				continue // Positive version-selection case above covers discovery 404.
			}
			t.Run(fmt.Sprintf("%s/%d", operation, code), func(t *testing.T) {
				var mu sync.Mutex
				var paths []string
				client := pdbHTTPClient(t, func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					paths = append(paths, r.URL.Path)
					mu.Unlock()
					if operation != "discovery" && r.URL.Path == "/apis/policy/v1" {
						pdbDiscovery(w, "v1", "list", "watch")
						return
					}
					pdbStatus(w, code)
				})
				pdbs, err := NewPodDisruptionBudgetClient(client, "ns")
				if err == nil && operation == "list" {
					_, err = pdbs.List(context.Background(), metav1.ListOptions{ResourceVersion: "42", ResourceVersionMatch: metav1.ResourceVersionMatchExact})
				}
				if err == nil && operation == "watch" {
					_, err = pdbs.Watch(context.Background(), metav1.ListOptions{ResourceVersion: "42"})
				}
				if !checks[code](err) || !strings.Contains(err.Error(), "original API error") {
					t.Fatalf("original API status lost: %v", err)
				}
				mu.Lock()
				defer mu.Unlock()
				for _, path := range paths {
					if strings.Contains(path, "v1beta1") {
						t.Fatalf("failed request downgraded: %v", paths)
					}
				}
			})
		}
	}
}

func TestPDBHTTPDiscoveryRequiresBothVerbs(t *testing.T) {
	for _, verbs := range [][]string{{"list"}, {"watch"}, {"get"}} {
		client := pdbHTTPClient(t, func(w http.ResponseWriter, r *http.Request) { pdbDiscovery(w, "v1", verbs...) })
		if _, err := NewPodDisruptionBudgetClient(client, "ns"); err == nil {
			t.Fatalf("accepted incomplete verbs: %v", verbs)
		}
	}
}

func TestPDBHTTPInformerUsesOneObjectType(t *testing.T) {
	// Match GenericClientSet startup: Kubernetes 1.18 supports ordinary
	// LIST/WATCH, but not the WatchList initial-events protocol.
	originalGates := clientfeatures.FeatureGates()
	clientfeatures.ReplaceFeatureGates(clientFeatureDefaults{base: originalGates, watchList: false})
	defer clientfeatures.ReplaceFeatureGates(originalGates)
	for _, version := range []string{"v1beta1", "v1"} {
		t.Run(version, func(t *testing.T) {
			fixtures := pdbFixtures(version)
			client := pdbHTTPClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("sendInitialEvents") == "true" {
					t.Error("ordinary PDB informer unexpectedly requested WatchList")
				}
				if r.URL.Path == "/apis/policy/v1" && version == "v1beta1" {
					pdbStatus(w, 404)
					return
				}
				if r.URL.Path == "/apis/policy/"+version {
					pdbDiscovery(w, version, "list", "watch")
					return
				}
				if r.URL.Query().Get("watch") == "true" {
					fixtures[0]["metadata"].(map[string]interface{})["resourceVersion"] = "43"
					pdbWriteJSON(w, map[string]interface{}{"type": "MODIFIED", "object": fixtures[0]})
					w.(http.Flusher).Flush()
					<-r.Context().Done()
					return
				}
				pdbWriteJSON(w, map[string]interface{}{"apiVersion": "policy/" + version, "kind": "PodDisruptionBudgetList", "metadata": map[string]string{"resourceVersion": "42"}, "items": fixtures})
			})
			pdbs, err := NewPodDisruptionBudgetClient(client, "ns")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			informer := cache.NewSharedIndexInformer(&cache.ListWatch{
				ListWithContextFunc: func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
					return pdbs.List(ctx, opts)
				},
				WatchFuncWithContext: pdbs.Watch,
			}, &policyv1.PodDisruptionBudget{}, 0, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
			updated := make(chan struct{}, 1)
			_, err = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{UpdateFunc: func(_, obj interface{}) {
				if pdb, ok := obj.(*policyv1.PodDisruptionBudget); ok && pdb.ResourceVersion == "43" {
					select {
					case updated <- struct{}{}:
					default:
					}
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() { defer close(done); informer.Run(ctx.Done()) }()
			defer func() { cancel(); <-done }()
			if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
				t.Fatal("PDB informer failed initial LIST sync")
			}
			select {
			case <-updated:
			case <-ctx.Done():
				t.Fatal("PDB informer failed WATCH update")
			}
			if len(informer.GetStore().List()) != 4 {
				t.Fatal("PDB informer lost initial items")
			}
		})
	}
}

func TestPDBWatchStopWithoutReader(t *testing.T) {
	for i := 0; i < 25; i++ {
		incoming := watch.NewRaceFreeFake()
		w := &pdbWatch{incoming: incoming, result: make(chan watch.Event), done: make(chan struct{})}
		go w.run(context.Background())
		incoming.Error(&metav1.Status{Code: 410})
		var wg sync.WaitGroup
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() { defer wg.Done(); w.Stop() }()
		}
		wg.Wait()
		select {
		case _, ok := <-w.ResultChan():
			if ok {
				for range w.ResultChan() {
				}
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Stop leaked a watch blocked on an unread result")
		}
	}
}
