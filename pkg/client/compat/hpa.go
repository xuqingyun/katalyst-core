package compat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	v2 "k8s.io/api/autoscaling/v2"
	beta "k8s.io/api/autoscaling/v2beta2"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// HPAToBeta preserves External/Object/Pods/Resource metrics and scaling behavior.
// Fields that cannot be represented by the beta schema are rejected, never silently dropped.
func HPAToBeta(in *v2.HorizontalPodAutoscaler) (*beta.HorizontalPodAutoscaler, error) {
	data, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	out := new(beta.HorizontalPodAutoscaler)
	if err = json.Unmarshal(data, out); err != nil {
		return nil, err
	}
	round, err := HPAFromBeta(out)
	if err != nil {
		return nil, err
	}
	before, _ := json.Marshal(in.Spec)
	after, _ := json.Marshal(round.Spec)
	if !bytes.Equal(before, after) {
		return nil, fmt.Errorf("HPA uses spec fields not representable in autoscaling/v2beta2")
	}
	out.APIVersion = "autoscaling/v2beta2"
	out.Kind = "HorizontalPodAutoscaler"
	return out, nil
}
func HPAFromBeta(in *beta.HorizontalPodAutoscaler) (*v2.HorizontalPodAutoscaler, error) {
	data, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	out := new(v2.HorizontalPodAutoscaler)
	if err = json.Unmarshal(data, out); err != nil {
		return nil, err
	}
	out.APIVersion = "autoscaling/v2"
	out.Kind = "HorizontalPodAutoscaler"
	return out, nil
}
func HPABetaListWatch(client kubernetes.Interface, namespace string) *cache.ListWatch {
	return &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
			list, err := client.AutoscalingV2beta2().HorizontalPodAutoscalers(namespace).List(ctx, opts)
			if err != nil {
				return nil, err
			}
			out := &v2.HorizontalPodAutoscalerList{ListMeta: list.ListMeta}
			for _, item := range list.Items {
				converted, e := HPAFromBeta(&item)
				if e != nil {
					return nil, e
				}
				out.Items = append(out.Items, *converted)
			}
			return out, nil
		},
		WatchFuncWithContext: func(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
			stream, err := client.AutoscalingV2beta2().HorizontalPodAutoscalers(namespace).Watch(ctx, opts)
			if err != nil {
				return nil, err
			}
			return watch.Filter(stream, func(event watch.Event) (watch.Event, bool) {
				if event.Type == watch.Error {
					return event, true
				}
				in, ok := event.Object.(*beta.HorizontalPodAutoscaler)
				if !ok {
					return watch.Event{Type: watch.Error, Object: &metav1.Status{Status: metav1.StatusFailure, Message: "unexpected HPA watch object", Code: 500}}, true
				}
				out, e := HPAFromBeta(in)
				if e != nil {
					return watch.Event{Type: watch.Error, Object: &metav1.Status{Status: metav1.StatusFailure, Message: e.Error(), Code: 500}}, true
				}
				event.Object = out
				return event, true
			}), nil
		},
	}
}
