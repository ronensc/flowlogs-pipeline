/*
 * Copyright (C) 2022 IBM, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package conntrack

import (
	"testing"

	"github.com/netobserv/flowlogs-pipeline/pkg/api"
	"github.com/stretchr/testify/require"
)

func TestNewAggregator_Invalid(t *testing.T) {
	// Empty Name
	var err error
	_, err = NewAggregator(api.OutputField{
		Operation: "sum",
		SplitAB:   true,
		Input:     "Input",
	})
	require.NotNil(t, err)

	// unknown Operation
	_, err = NewAggregator(api.OutputField{
		Name:      "MyAgg",
		Operation: "unknown",
		SplitAB:   true,
		Input:     "Input",
	})
}

func TestNewAggregator_Valid(t *testing.T) {
	table := []struct {
		name        string
		outputField api.OutputField
		expected    aggregator
	}{
		{
			name:        "Default SplitAB",
			outputField: api.OutputField{Name: "MyAgg", Operation: "sum"},
			expected:    aggregateSum{aggregateBase{"MyAgg", "MyAgg", false}},
		},
		{
			name:        "Default input",
			outputField: api.OutputField{Name: "MyAgg", Operation: "sum", SplitAB: true},
			expected:    aggregateSum{aggregateBase{"MyAgg", "MyAgg", true}},
		},
		{
			name:        "Custom input",
			outputField: api.OutputField{Name: "MyAgg", Operation: "sum", Input: "MyInput"},
			expected:    aggregateSum{aggregateBase{"MyInput", "MyAgg", false}},
		},
		{
			name:        "Operation count",
			outputField: api.OutputField{Name: "MyAgg", Operation: "count"},
			expected:    aggregateCount{aggregateBase{"MyAgg", "MyAgg", false}},
		},
		{
			name:        "Operation max",
			outputField: api.OutputField{Name: "MyAgg", Operation: "max"},
			expected:    aggregateMax{aggregateBase{"MyAgg", "MyAgg", false}},
		},
		{
			name:        "Operation min",
			outputField: api.OutputField{Name: "MyAgg", Operation: "min"},
			expected:    aggregateMin{aggregateBase{"MyAgg", "MyAgg", false}},
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			agg, err := NewAggregator(test.outputField)
			require.NoError(t, err)
			require.Equal(t, test.expected, agg)
		})
	}
}

// TODO: rename
func TestTEMP(t *testing.T) {
	var agg aggregator
	agg, _ = NewAggregator(api.OutputField{
		Name:      "MyField",
		Operation: "sum",
		SplitAB:   true,
		Input:     "Input",
	})
	// Test NewAggregator
	// Test default Input
	// Test splitAB

	kd := api.KeyDefinition{}
	fl := NewFlowLog("a", 1, "b", 2, 3, 4, 5)
	hash, err := ComputeHash(fl, kd, hasher)
	if err != nil {

	}
	conn := NewConn(fl, hash)

	// Test AddField
	agg.addField(conn)
	x, ok := conn.(connType).aggFields["MyField"]
	require.True(t, ok)
	require.Equal(t, x, 0)

	// Test update on each agg type!
	agg.update(conn, fl, dirNA)
}
