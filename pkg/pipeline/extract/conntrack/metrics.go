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
	"github.com/netobserv/flowlogs-pipeline/pkg/operational"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	connStoreRunningLengthDef = operational.DefineMetric(
		"conntrack_memory_connections_running",
		"The total number of tracked running connections in memory.",
		operational.TypeGauge,
		"group",
	)

	connStoreExpiredLengthDef = operational.DefineMetric(
		"conntrack_memory_connections_expired",
		"The total number of tracked expired connections in memory.",
		operational.TypeGauge,
		"group",
	)

	inputRecordsDef = operational.DefineMetric(
		"conntrack_input_records",
		"The total number of input records per classification.",
		operational.TypeCounter,
		"classification",
	)

	outputRecordsDef = operational.DefineMetric(
		"conntrack_output_records",
		"The total number of output records.",
		operational.TypeCounter,
		"type",
	)

	tcpFlagsDef = operational.DefineMetric(
		"conntrack_tcp_flags",
		"The total number of actions taken based on TCP flags.",
		operational.TypeCounter,
		"action",
	)
)

type metricsType struct {
	runningConnStoreLength *prometheus.GaugeVec
	expiredConnStoreLength *prometheus.GaugeVec
	inputRecords           *prometheus.CounterVec
	outputRecords          *prometheus.CounterVec
	tcpFlags               *prometheus.CounterVec
}

func newMetrics(opMetrics *operational.Metrics) *metricsType {
	return &metricsType{
		runningConnStoreLength: opMetrics.NewGaugeVec(&connStoreRunningLengthDef),
		expiredConnStoreLength: opMetrics.NewGaugeVec(&connStoreExpiredLengthDef),
		inputRecords:           opMetrics.NewCounterVec(&inputRecordsDef),
		outputRecords:          opMetrics.NewCounterVec(&outputRecordsDef),
		tcpFlags:               opMetrics.NewCounterVec(&tcpFlagsDef),
	}
}
