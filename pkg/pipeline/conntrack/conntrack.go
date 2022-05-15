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
	"encoding/hex"
	"fmt"
	"hash"
	"hash/fnv"
	"math"

	"github.com/netobserv/flowlogs-pipeline/pkg/api"
	"github.com/netobserv/flowlogs-pipeline/pkg/config"
	"github.com/netobserv/flowlogs-pipeline/pkg/pipeline/utils"
	log "github.com/sirupsen/logrus"
)

type ConnectionTracker interface {
	ConnectionTrack(flowLogs []config.GenericMap) []config.GenericMap
}

//////////////////////////////////////
// TODO: Move aggregators to a file
type aggregator interface {
	update(impl connType, flowLog config.GenericMap)
}

type aggregateBase struct {
	inputField  string
	outputField string
}

type aggregateSum struct {
	aggregateBase
}

type aggregateCount struct {
	aggregateBase
}

type aggregateMin struct {
	aggregateBase
}

type aggregateMax struct {
	aggregateBase
}

func (agg aggregateSum) update(conn connType, flowLog config.GenericMap) {
	aggValue, err := utils.ConvertToFloat64(flowLog[agg.inputField])
	if err != nil {
		// TODO: log...
	}
	conn.aggFields[agg.outputField] += aggValue
}

func (agg aggregateCount) update(conn connType, flowLog config.GenericMap) {
	conn.aggFields[agg.outputField]++
}

func (agg aggregateMin) update(conn connType, flowLog config.GenericMap) {
	aggValue, err := utils.ConvertToFloat64(flowLog[agg.inputField])
	if err != nil {
		// TODO: log...
	}
	conn.aggFields[agg.outputField] = math.Min(conn.aggFields[agg.outputField], aggValue)
}

func (agg aggregateMax) update(conn connType, flowLog config.GenericMap) {
	aggValue, err := utils.ConvertToFloat64(flowLog[agg.inputField])
	if err != nil {
		// TODO: log...
	}
	conn.aggFields[agg.outputField] = math.Max(conn.aggFields[agg.outputField], aggValue)
}

//////////////////////////////////////

type conntrackImpl struct {
	config api.ConnTrack
	hasher hash.Hash
	// TODO: use type hash?
	hash2conn   map[string]connType
	aggregators []aggregator
}

type hashStr string

type connType struct {
	hash      hashStr
	keys      config.GenericMap
	aggFields map[string]float64
}

func (c connType) toGenericMap() config.GenericMap {
	// TODO:
	return config.GenericMap{}
}

// TODO: Decode decodes input strings to a list of flow entries
func (ct *conntrackImpl) ConnectionTrack(flowLogs []config.GenericMap) []config.GenericMap {
	log.Debugf("Entering ConnectionTrack")
	log.Debugf("ConnectionTrack none, in = %v", flowLogs)

	var outputRecords []config.GenericMap
	for _, fl := range flowLogs {
		// TODO: think of returning a string rather than []byte
		hash, err := ComputeHash(fl, ct.config.KeyDefinition, ct.hasher)
		if err != nil {
			// TODO: handle error
			continue
		}
		hashStr := hex.EncodeToString(hash)
		conn, exists := ct.hash2conn[hashStr]
		if !exists {
			conn = NewConn(fl)
			ct.addConnection(hashStr, conn)
			outputRecords = append(outputRecords, conn.toGenericMap())
		} else {
			ct.updateConnection(conn, fl)
		}
	}
	return outputRecords
}

func (ct conntrackImpl) addConnection(hashStr string, conn connType) {
	// TODO:
	ct.hash2conn[hashStr] = conn
}

func (ct conntrackImpl) updateConnection(conn connType, flowLog config.GenericMap) {
	for _, agg := range ct.aggregators {
		agg.update(conn, flowLog)
	}
}

func NewConn(flowLog config.GenericMap) connType {
	// TODO
	return connType{}
}

// TODO: NewDecodeNone create a new decode
func NewConnectionTrack(config api.ConnTrack) (ConnectionTracker, error) {
	var aggregators []aggregator
	for _, of := range config.OutputFields {
		var inputField string
		if of.Input != "" {
			inputField = of.Input
		} else {
			inputField = of.Name
		}
		aggBase := aggregateBase{inputField: inputField, outputField: of.Name}
		var agg aggregator
		switch of.Operation {
		case "sum":
			agg = aggregateSum{aggBase}
		case "count":
			agg = aggregateCount{aggBase}
		case "min":
			agg = aggregateMin{aggBase}
		case "max":
			agg = aggregateMax{aggBase}
		default:
			return nil, fmt.Errorf("unknown operation: %q", of.Operation)
		}
		aggregators = append(aggregators, agg)
	}

	conntrack := &conntrackImpl{
		config:      config,
		hasher:      fnv.New32a(),
		aggregators: aggregators,
	}
	return conntrack, nil
}

/////////////////////////////////////////////////////////////////
type conntrackNone struct {
}

// TODO: Decode decodes input strings to a list of flow entries
func (ct *conntrackNone) ConnectionTrack(flowLogs []config.GenericMap) []config.GenericMap {
	log.Debugf("Entering ConnectionTrack none")
	log.Debugf("ConnectionTrack none, in = %v", flowLogs)
	var f []config.GenericMap
	return f
}

// TODO: NewDecodeNone create a new decode
func NewConnectionTrackNone() (ConnectionTracker, error) {
	return &conntrackNone{}, nil
}
