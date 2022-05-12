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

	"github.com/netobserv/flowlogs-pipeline/pkg/api"
	"github.com/netobserv/flowlogs-pipeline/pkg/config"
	log "github.com/sirupsen/logrus"
)

type ConnectionTracker interface {
	ConnectionTrack(flowLogs []config.GenericMap) []config.GenericMap
}

type conntrackImpl struct {
	config api.ConnTrack
	// TODO: use type hash?
	hash2conn map[string]connType
}

type hash string

type connType struct {
	hash      hash
	keys      config.GenericMap
	aggFields config.GenericMap
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
		hash, err := ComputeHash(fl, ct.config.KeyFields)
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

	}

	for _, of := range ct.config.OutputFields {
		var inputField string
		// TODO: define aggregators and do the following if only once.
		if of.Input != "" {
			inputField = of.Input
		} else {
			inputField = of.Name
		}
		flowLog[inputField]

	}
}

func NewConn(flowLog config.GenericMap) connType {
	// TODO
	return connType{}
}

// TODO: NewDecodeNone create a new decode
func NewConnectionTrack(config api.ConnTrack) (ConnectionTracker, error) {
	return &conntrackImpl{config: config}, nil
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
