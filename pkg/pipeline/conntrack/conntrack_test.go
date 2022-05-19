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
	"fmt"
	"testing"

	"github.com/netobserv/flowlogs-pipeline/pkg/api"
	"github.com/netobserv/flowlogs-pipeline/pkg/config"
	"github.com/stretchr/testify/require"
)

func buildConnTrackConfig(bidi bool, outputRecordType []string) *api.ConnTrack {
	splitAB := bidi
	var h api.ConnTrackHash
	if bidi {
		h = api.ConnTrackHash{
			FieldGroupRefs: []string{"protocol"},
			FieldGroupARef: "src",
			FieldGroupBRef: "dst",
		}
	} else {
		h = api.ConnTrackHash{
			FieldGroupRefs: []string{"protocol", "src", "dst"},
		}
	}
	return &api.ConnTrack{
		KeyDefinition: api.KeyDefinition{
			FieldGroups: []api.FieldGroup{
				{
					Name: "src",
					Fields: []string{
						"SrcAddr",
						"SrcPort",
					},
				},
				{
					Name: "dst",
					Fields: []string{
						"DstAddr",
						"DstPort",
					},
				},
				{
					Name: "protocol",
					Fields: []string{
						"Proto",
					},
				},
			},
			Hash: h,
		},
		OutputFields: []api.OutputField{
			{Name: "Bytes", Operation: "sum", SplitAB: splitAB},
			{Name: "Packets", Operation: "sum", SplitAB: splitAB},
			{Name: "numFlowLogs", Operation: "count", SplitAB: splitAB},
		},
		OutputRecordTypes: outputRecordType,
	}
}

func NewConnAB() config.GenericMap {
	return nil
}

func TestTrack(t *testing.T) {

	ipA := "10.0.0.1"
	ipB := "10.0.0.2"
	portA := 9001
	portB := 9002
	protocolA := 6

	fl1 := []config.GenericMap{
		NewFlowLog(ipA, portA, ipB, portB, protocolA, 111, 22),
		NewFlowLog(ipA, portA, ipB, portB, protocolA, 222, 11),
	}
	fl2 := []config.GenericMap{
		NewFlowLog(ipB, portB, ipA, portA, protocolA, 111, 22),
		NewFlowLog(ipB, portB, ipA, portA, protocolA, 111, 22),
	}
	conf := buildConnTrackConfig(false, nil)
	ct, err := NewConnectionTrack(*conf)
	require.NoError(t, err)

	fmt.Println(ct.Track(fl1))
	fmt.Println(ct.Track(fl2))

	fl3 := NewFlowLog(ipA, portA, ipB, portB, protocolA, 111, 22)
	fl4 := NewFlowLog(ipA, portA, ipB, portB, protocolA, 222, 11)
	table := []struct {
		name          string
		conf          *api.ConnTrack
		inputFlowLogs []config.GenericMap
		expected      []config.GenericMap
	}{
		// TODO: Complete details
		{
			"bidi, no flow logs",
			buildConnTrackConfig(true, []string{}),
			[]config.GenericMap{fl3, fl4},
			[]config.GenericMap{NewConnAB()},
		},
		{
			"bidi, with flow logs",
			buildConnTrackConfig(true, []string{"original"}),
			[]config.GenericMap{fl3, fl4},
			[]config.GenericMap{NewConnAB(), fl3, fl4},
		},
		{
			"unidi, no flow logs",
			buildConnTrackConfig(true, []string{}),
			[]config.GenericMap{fl3, fl4},
			[]config.GenericMap{NewConnAB(), NewConnAB()},
		},
		{
			"unidi, with flow logs",
			buildConnTrackConfig(true, []string{"original"}),
			[]config.GenericMap{fl3, fl4},
			[]config.GenericMap{NewConnAB(), fl3, NewConnAB(), fl4},
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			ct, err := NewConnectionTrack(*test.conf)
			require.NoError(t, err)
			actual := ct.Track(test.inputFlowLogs)
			require.Equal(t, test.expected, actual)
		})
	}
}
