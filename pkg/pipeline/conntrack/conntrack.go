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

	"github.com/netobserv/flowlogs-pipeline/pkg/api"
	"github.com/netobserv/flowlogs-pipeline/pkg/config"
	log "github.com/sirupsen/logrus"
)

type ConnectionTracker interface {
	Track(flowLogs []config.GenericMap) []config.GenericMap
}

type conntrackImpl struct {
	config api.ConnTrack
	hasher hash.Hash
	// TODO: should the key of the map be a custom hashStrType instead of string?
	hash2conn   map[string]connection
	aggregators []aggregator
	// TODO: rename to outputFlowLogs
	outputOriginal bool
}

func (ct *conntrackImpl) Track(flowLogs []config.GenericMap) []config.GenericMap {
	log.Debugf("Entering Track")
	log.Debugf("Track none, in = %v", flowLogs)

	var outputRecords []config.GenericMap
	for _, fl := range flowLogs {
		// TODO: think of returning a string rather than []byte
		computedHash, err := ComputeHash(fl, ct.config.KeyDefinition, ct.hasher)
		if err != nil {
			log.Warningf("skipping flow log: %v", err)
			continue
		}
		hashStr := hex.EncodeToString(computedHash.hashTotal)
		conn, exists := ct.hash2conn[hashStr]
		if !exists {
			builder := NewConnBuilder()
			conn = builder.
				Hash(computedHash).
				KeysFrom(fl, ct.config.KeyDefinition).
				Aggregators(ct.aggregators).
				Build()
			ct.addConnection(hashStr, conn)
			ct.updateConnection(conn, fl, computedHash)
			outputRecords = append(outputRecords, conn.toGenericMap())
		} else {
			ct.updateConnection(conn, fl, computedHash)
		}

		if ct.outputOriginal {
			outputRecords = append(outputRecords, fl)
		}
	}
	return outputRecords
}

func (ct *conntrackImpl) addConnection(hashStr string, conn connection) {
	ct.hash2conn[hashStr] = conn
}

type direction uint8

const (
	dirNA direction = iota
	dirAB
	dirBA
)

func (ct *conntrackImpl) getFlowLogDirection(conn connection, flowLogHash *totalHashType) direction {
	d := dirNA
	if ct.config.KeyDefinition.Hash.FieldGroupARef != "" {
		if hex.EncodeToString(conn.getHash().hashA) == hex.EncodeToString(flowLogHash.hashA) {
			// A -> B
			d = dirAB
		} else {
			// B -> A
			d = dirBA
		}
	}
	return d
}

func (ct *conntrackImpl) updateConnection(conn connection, flowLog config.GenericMap, flowLogHash *totalHashType) {
	d := ct.getFlowLogDirection(conn, flowLogHash)
	for _, agg := range ct.aggregators {
		agg.update(conn, flowLog, d)
	}
}

// NewConnectionTrack creates a new connection track instance
func NewConnectionTrack(config api.ConnTrack) (ConnectionTracker, error) {
	var aggregators []aggregator
	for _, of := range config.OutputFields {
		agg, err := NewAggregator(of)
		if err != nil {
			return nil, fmt.Errorf("error creating aggregator: %w", err)
		}
		aggregators = append(aggregators, agg)
	}
	outputOriginal := false
	for _, option := range config.OutputRecordTypes {
		switch option {
		case "outputOriginal":
			outputOriginal = true
		default:
			return nil, fmt.Errorf("unknown OutputRecordTypes: %v", option)
		}
	}

	conntrack := &conntrackImpl{
		config:         config,
		hasher:         fnv.New32a(),
		aggregators:    aggregators,
		hash2conn:      make(map[string]connection),
		outputOriginal: outputOriginal,
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
