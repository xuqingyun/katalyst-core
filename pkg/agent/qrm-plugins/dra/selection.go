// Copyright 2026 The Katalyst Authors. SPDX-License-Identifier: Apache-2.0
package dra

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const providerModelsKey = "provider-models"

// A configured list replaces that provider's defaults. Other compiled provider
// defaults remain available; [] disables matching without another enable flag.
func resolveProviderModels(config Config, providers map[string]Provider) (map[string][]string, error) {
	if _, old := config["model-providers"]; old {
		return nil, fmt.Errorf("DRA model-providers was replaced by provider-models (provider to model list)")
	}
	models := make(map[string][]string, len(providers))
	for name, p := range providers {
		models[name] = append([]string(nil), p.DefaultModels...)
	}
	if raw, configured := config[providerModelsKey]; configured {
		var explicit map[string][]string
		if err := json.Unmarshal(raw, &explicit); err != nil {
			return nil, fmt.Errorf("DRA provider-models must map provider names to model lists: %w", err)
		}
		if explicit == nil {
			return nil, fmt.Errorf("DRA provider-models must be an object")
		}
		for name, list := range explicit {
			if providers[name].New == nil {
				return nil, fmt.Errorf("DRA provider %q in provider-models is not compiled into this agent", name)
			}
			if list == nil {
				return nil, fmt.Errorf("DRA provider-models.%s must be a model list; use [] to disable matching", name)
			}
			models[name] = append([]string(nil), list...)
		}
	}
	names := make([]string, 0, len(models))
	for name := range models {
		names = append(names, name)
	}
	sort.Strings(names)
	owners := make(map[string]string)
	for _, name := range names {
		for i, model := range models[name] {
			model = strings.ToUpper(strings.TrimSpace(model))
			if model == "" {
				return nil, fmt.Errorf("DRA provider %s has an empty model name", name)
			}
			if previous, exists := owners[model]; exists {
				return nil, fmt.Errorf("DRA model %s appears more than once (%s, %s)", model, previous, name)
			}
			owners[model] = name
			models[name][i] = model
		}
		sort.Strings(models[name])
	}
	return models, nil
}
