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
	"github.com/netobserv/flowlogs-pipeline/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNewCopier_Invalid(t *testing.T) {
	var err error

	// Empty Name
	_, err = newCopier(api.OutputField{
		Operation: "copyFirst",
		SplitAB:   true,
		Input:     "Input",
	})
	require.NotNil(t, err)

	// unknown OperationType
	_, err = newCopier(api.OutputField{
		Name:      "MyCp",
		Operation: "unknown",
		SplitAB:   true,
		Input:     "Input",
	})
	require.NotNil(t, err)
}

func TestNewCopier_Valid(t *testing.T) {
	table := []struct {
		name        string
		outputField api.OutputField
		expected    copier
	}{
		{
			name:        "Default first",
			outputField: api.OutputField{Name: "MyCp", Operation: "copyFirst"},
			expected:    &cFirst{copyBase{"MyCp", "MyCp"}},
		},
		{
			name:        "Custom input first",
			outputField: api.OutputField{Name: "MyCp", Operation: "copyFirst", Input: "MyInput"},
			expected:    &cFirst{copyBase{"MyInput", "MyCp"}},
		},
		{
			name:        "Default last",
			outputField: api.OutputField{Name: "MyCp", Operation: "copyLast"},
			expected:    &cLast{copyBase{"MyCp", "MyCp"}},
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			c, err := newCopier(test.outputField)
			require.NoError(t, err)
			require.Equal(t, test.expected, c)
		})
	}
}

func TestCopier_UpdateFirst(t *testing.T) {
	ofs := []api.OutputField{
		{Input: "FlowDirection", Name: "FirstFlowDirection", Operation: "copyFirst"},
		{Input: "FlowDirection", Name: "LastFlowDirection", Operation: "copyLast"},
	}
	var cs []copier
	for _, of := range ofs {
		c, err := newCopier(of)
		require.NoError(t, err)
		cs = append(cs, c)
	}

	ipA := "10.0.0.1"
	ipB := "10.0.0.2"
	portA := 1
	portB := 9002
	protocolA := 6
	flowDirA := 0
	flowDirB := 1

	table := []struct {
		name     string
		flowLog  config.GenericMap
		expected map[string]interface{}
	}{
		{
			name:     "flowLog 1",
			flowLog:  newMockFlowLog(ipA, portA, ipB, portB, protocolA, flowDirA, 100, 10, false),
			expected: map[string]interface{}{"FirstFlowDirection": 0, "LastFlowDirection": 0},
		},
		{
			name:     "flowLog 2",
			flowLog:  newMockFlowLog(ipA, portA, ipB, portB, protocolA, flowDirB, 200, 20, false),
			expected: map[string]interface{}{"FirstFlowDirection": 0, "LastFlowDirection": 1},
		},
	}

	conn := NewConnBuilder(nil).Build()
	for _, agg := range cs {
		agg.addField(conn)
	}
	expectedInits := map[string]interface{}{"FirstFlowDirection": nil, "LastFlowDirection": nil}
	require.Equal(t, expectedInits, conn.(*connType).cpFields)

	for i, test := range table {
		t.Run(test.name, func(t *testing.T) {
			for _, cp := range cs {
				cp.update(conn, test.flowLog, i == 0)
			}
			require.Equal(t, test.expected, conn.(*connType).cpFields)
		})
	}
}
