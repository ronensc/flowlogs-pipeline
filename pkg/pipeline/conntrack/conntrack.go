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
	Track(flowLogs []config.GenericMap) []config.GenericMap
}

//////////////////////////////////////
// TODO: Move aggregators to a file
type aggregator interface {
	addField(impl connType)
	update(impl connType, flowLog config.GenericMap, d direction)
}

type aggregateBase struct {
	inputField  string
	outputField string
	splitAB     bool
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

func (agg aggregateBase) getOutputField(d direction) string {
	outputField := agg.outputField
	if agg.splitAB {
		switch d {
		case dirAB:
			outputField += "_AB"
		case dirBA:
			outputField += "_BA"
		default:
			log.Panicf("splitAB aggregator %v cannot determine outputField because direction is missing. Check configuration.", outputField)
		}
	}
	return outputField
}

func (agg aggregateBase) addField(conn connType) {
	if agg.splitAB {
		conn.aggFields[agg.getOutputField(dirAB)] = 0
		conn.aggFields[agg.getOutputField(dirBA)] = 0
	} else {
		conn.aggFields[agg.getOutputField(dirNA)] = 0
	}
}

func (agg aggregateBase) getInputFieldValue(flowLog config.GenericMap) (float64, error) {
	rawValue, ok := flowLog[agg.inputField]
	if !ok {
		return 0, fmt.Errorf("missing field %v", agg.inputField)
	}
	floatValue, err := utils.ConvertToFloat64(rawValue)
	if err != nil {
		return 0, fmt.Errorf("cannot convert %v to float64: %w", rawValue, err)
	}
	return floatValue, nil
}

func (agg aggregateSum) update(conn connType, flowLog config.GenericMap, d direction) {
	outputField := agg.getOutputField(d)
	v, err := agg.getInputFieldValue(flowLog)
	if err != nil {
		log.Errorf("error updating connection %v: %v", string(conn.hash.hashTotal), err)
		return
	}
	conn.aggFields[outputField] += v
}

func (agg aggregateCount) update(conn connType, flowLog config.GenericMap, d direction) {
	outputField := agg.getOutputField(d)
	conn.aggFields[outputField]++
}

func (agg aggregateMin) update(conn connType, flowLog config.GenericMap, d direction) {
	outputField := agg.getOutputField(d)
	v, err := agg.getInputFieldValue(flowLog)
	if err != nil {
		log.Errorf("error updating connection %v: %v", string(conn.hash.hashTotal), err)
		return
	}
	conn.aggFields[outputField] = math.Min(conn.aggFields[outputField], v)
}

func (agg aggregateMax) update(conn connType, flowLog config.GenericMap, d direction) {
	outputField := agg.getOutputField(d)
	v, err := agg.getInputFieldValue(flowLog)
	if err != nil {
		log.Errorf("error updating connection %v: %v", string(conn.hash.hashTotal), err)
		return
	}
	conn.aggFields[outputField] = math.Max(conn.aggFields[outputField], v)
}

//////////////////////////////////////

type conntrackImpl struct {
	config api.ConnTrack
	hasher hash.Hash
	// TODO: should the key of the map be a custom hashStrType instead of string?
	hash2conn   map[string]connType
	aggregators []aggregator
}

type connType struct {
	hash      *totalHashType
	keys      config.GenericMap
	aggFields map[string]float64
}

func (c connType) toGenericMap() config.GenericMap {
	gm := config.GenericMap{}
	for k, v := range c.aggFields {
		gm[k] = v
	}
	// In case of a conflict between the keys and the aggFields, the keys should prevail.
	for k, v := range c.keys {
		gm[k] = v
	}
	return gm
}

func (ct *conntrackImpl) Track(flowLogs []config.GenericMap) []config.GenericMap {
	log.Debugf("Entering Track")
	log.Debugf("Track none, in = %v", flowLogs)

	var outputRecords []config.GenericMap
	for _, fl := range flowLogs {
		// TODO: think of returning a string rather than []byte
		hash, err := ComputeHash(fl, ct.config.KeyDefinition, ct.hasher)
		if err != nil {
			// TODO: handle error
			continue
		}
		hashStr := hex.EncodeToString(hash.hashTotal)
		conn, exists := ct.hash2conn[hashStr]
		if !exists {
			conn = NewConn(fl, hash)
			ct.addConnection(hashStr, conn)
			outputRecords = append(outputRecords, conn.toGenericMap())
		} else {
			ct.updateConnection(conn, fl, hash)
		}
	}
	return outputRecords
}

func (ct conntrackImpl) addConnection(hashStr string, conn connType) {
	// TODO:
	for _, agg := range ct.aggregators {
		agg.addField(conn)
	}
	ct.hash2conn[hashStr] = conn
}

type direction uint8

const (
	dirNA direction = iota
	dirAB
	dirBA
)

func (ct conntrackImpl) getFlowLogDirection(conn connType, flowLogHash *totalHashType) direction {
	d := dirNA
	if ct.config.KeyDefinition.Hash.FieldGroupARef != "" {
		if hex.EncodeToString(conn.hash.hashA) == hex.EncodeToString(flowLogHash.hashA) {
			// A -> B
			d = dirAB
		} else {
			// B -> A
			d = dirBA
		}
	}
	return d
}

func (ct conntrackImpl) updateConnection(conn connType, flowLog config.GenericMap, flowLogHash *totalHashType) {
	d := ct.getFlowLogDirection(conn, flowLogHash)
	for _, agg := range ct.aggregators {
		agg.update(conn, flowLog, d)
	}
}

func NewConn(flowLog config.GenericMap, hash *totalHashType) connType {
	// TODO: add keys
	return connType{hash: hash}
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
func (ct *conntrackNone) Track(flowLogs []config.GenericMap) []config.GenericMap {
	log.Debugf("Entering Track none")
	log.Debugf("Track none, in = %v", flowLogs)
	var f []config.GenericMap
	return f
}

// TODO: NewDecodeNone create a new decode
func NewConnectionTrackNone() (ConnectionTracker, error) {
	return &conntrackNone{}, nil
}
