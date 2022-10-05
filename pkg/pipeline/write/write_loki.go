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

package write

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/netobserv/flowlogs-pipeline/pkg/api"
	"github.com/netobserv/flowlogs-pipeline/pkg/config"
	"github.com/netobserv/flowlogs-pipeline/pkg/operational"
	pUtils "github.com/netobserv/flowlogs-pipeline/pkg/pipeline/utils"

	logAdapter "github.com/go-kit/kit/log/logrus"
	jsonIter "github.com/json-iterator/go"
	"github.com/netobserv/loki-client-go/loki"
	"github.com/netobserv/loki-client-go/pkg/backoff"
	"github.com/netobserv/loki-client-go/pkg/urlutil"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
)

var (
	keyReplacer = strings.NewReplacer("/", "_", ".", "_", "-", "_")
)

type emitter interface {
	Handle(labels model.LabelSet, timestamp time.Time, record string) error
}

const channelSize = 1000

// Loki record writer
type Loki struct {
	lokiConfig     loki.Config
	apiConfig      api.WriteLoki
	timestampScale float64
	saneLabels     map[string]model.LabelName
	client         emitter
	timeNow        func() time.Time
	in             chan config.GenericMap
	exitChan       <-chan struct{}
	metrics        *metrics
}

func buildLokiConfig(c *api.WriteLoki) (loki.Config, error) {
	batchWait, err := time.ParseDuration(c.BatchWait)
	if err != nil {
		return loki.Config{}, fmt.Errorf("failed in parsing BatchWait : %v", err)
	}

	timeout, err := time.ParseDuration(c.Timeout)
	if err != nil {
		return loki.Config{}, fmt.Errorf("failed in parsing Timeout : %v", err)
	}

	minBackoff, err := time.ParseDuration(c.MinBackoff)
	if err != nil {
		return loki.Config{}, fmt.Errorf("failed in parsing MinBackoff : %v", err)
	}

	maxBackoff, err := time.ParseDuration(c.MaxBackoff)
	if err != nil {
		return loki.Config{}, fmt.Errorf("failed in parsing MaxBackoff : %v", err)
	}

	cfg := loki.Config{
		TenantID:  c.TenantID,
		BatchWait: batchWait,
		BatchSize: c.BatchSize,
		Timeout:   timeout,
		BackoffConfig: backoff.BackoffConfig{
			MinBackoff: minBackoff,
			MaxBackoff: maxBackoff,
			MaxRetries: c.MaxRetries,
		},
	}
	if c.ClientConfig != nil {
		cfg.Client = *c.ClientConfig
	}
	var clientURL urlutil.URLValue
	err = clientURL.Set(strings.TrimSuffix(c.URL, "/") + "/loki/api/v1/push")
	if err != nil {
		return cfg, fmt.Errorf("failed to parse client URL: %w", err)
	}
	cfg.URL = clientURL
	return cfg, nil
}

func (l *Loki) ProcessRecord(in config.GenericMap) error {
	// copy record before process to avoid alteration on parallel stages
	out := in.Copy()
	labels := model.LabelSet{}

	// Add static labels from config
	for k, v := range l.apiConfig.StaticLabels {
		labels[k] = v
	}
	l.addLabels(in, labels)

	// Remove labels and configured ignore list from record
	ignoreList := append(l.apiConfig.IgnoreList, l.apiConfig.Labels...)
	for _, label := range ignoreList {
		delete(out, label)
	}

	js, err := jsonIter.ConfigCompatibleWithStandardLibrary.Marshal(out)
	if err != nil {
		return err
	}

	timestamp := l.extractTimestamp(out)
	err = l.client.Handle(labels, timestamp, string(js))
	if err == nil {
		l.metrics.recordsWritten.Inc()
	}
	return err
}

func (l *Loki) extractTimestamp(record map[string]interface{}) time.Time {
	if l.apiConfig.TimestampLabel == "" {
		return l.timeNow()
	}
	timestamp, ok := record[string(l.apiConfig.TimestampLabel)]
	if !ok {
		log.WithField("timestampLabel", l.apiConfig.TimestampLabel).
			Warnf("Timestamp label not found in record. Using local time")
		return l.timeNow()
	}
	ft, ok := getFloat64(timestamp)
	if !ok {
		log.WithField(string(l.apiConfig.TimestampLabel), timestamp).
			Warnf("Invalid timestamp found: float64 expected but got %T. Using local time", timestamp)
		return l.timeNow()
	}
	if ft == 0 {
		log.WithField("timestampLabel", l.apiConfig.TimestampLabel).
			Warnf("Empty timestamp in record. Using local time")
		return l.timeNow()
	}

	tsNanos := int64(ft * l.timestampScale)
	return time.Unix(tsNanos/int64(time.Second), tsNanos%int64(time.Second))
}

func (l *Loki) addLabels(record config.GenericMap, labels model.LabelSet) {
	// Add non-static labels from record
	for _, label := range l.apiConfig.Labels {
		val, ok := record[label]
		if !ok {
			continue
		}
		sanitized, ok := l.saneLabels[label]
		if !ok {
			continue
		}
		lv := model.LabelValue(fmt.Sprint(val))
		if !lv.IsValid() {
			log.WithFields(log.Fields{"key": label, "value": val}).
				Debug("Invalid label value. Ignoring it")
			continue
		}
		labels[sanitized] = lv
	}
}

func getFloat64(timestamp interface{}) (ft float64, ok bool) {
	switch i := timestamp.(type) {
	case float64:
		return i, true
	case float32:
		return float64(i), true
	case int64:
		return float64(i), true
	case int32:
		return float64(i), true
	case uint64:
		return float64(i), true
	case uint32:
		return float64(i), true
	case int:
		return float64(i), true
	default:
		log.Warnf("Type %T is not implemented for float64 conversion\n", i)
		return math.NaN(), false
	}
}

// Write writes a flow before being stored
func (l *Loki) Write(entry config.GenericMap) {
	log.Debugf("entering Loki Write")
	l.in <- entry
}

func (l *Loki) processRecords() {
	for {
		select {
		case <-l.exitChan:
			log.Debugf("exiting writeLoki because of signal")
			return
		case record := <-l.in:
			err := l.ProcessRecord(record)
			if err != nil {
				log.Errorf("Write (Loki) error %v", err)
			}
		}
	}
}

// NewWriteLoki creates a Loki writer from configuration
func NewWriteLoki(opMetrics *operational.Metrics, params config.StageParam) (*Loki, error) {
	log.Debugf("entering NewWriteLoki")
	lokiConfigIn := api.WriteLoki{}
	if params.Write != nil && params.Write.Loki != nil {
		lokiConfigIn = *params.Write.Loki
	}
	// need to combine defaults with parameters that are provided in the config yaml file
	lokiConfigIn.SetDefaults()

	if err := lokiConfigIn.Validate(); err != nil {
		return nil, fmt.Errorf("the provided config is not valid: %w", err)
	}

	lokiConfig, buildconfigErr := buildLokiConfig(&lokiConfigIn)
	if buildconfigErr != nil {
		return nil, buildconfigErr
	}
	client, newWithLoggerErr := loki.NewWithLogger(lokiConfig, logAdapter.NewLogger(log.WithField("module", "export/loki")))
	if newWithLoggerErr != nil {
		return nil, newWithLoggerErr
	}

	timestampScale, err := time.ParseDuration(lokiConfigIn.TimestampScale)
	if err != nil {
		return nil, fmt.Errorf("cannot parse TimestampScale: %w", err)
	}

	// Sanitize label keys
	saneLabels := make(map[string]model.LabelName, len(lokiConfigIn.Labels))
	for _, label := range lokiConfigIn.Labels {
		sanitized := model.LabelName(keyReplacer.Replace(label))
		if sanitized.IsValid() {
			saneLabels[label] = sanitized
		} else {
			log.WithFields(log.Fields{"key": label, "sanitized": sanitized}).
				Debug("Invalid label. Ignoring it")
		}
	}

	// TODO / FIXME / FIGUREOUT: seems like we have 2 input channels for Loki? (this one, and see also pipeline_builder.go / getStageNode / StageWrite)
	in := make(chan config.GenericMap, channelSize)
	opMetrics.CreateInQueueSizeGauge(params.Name+"-2", func() int { return len(in) })

	l := &Loki{
		lokiConfig:     lokiConfig,
		apiConfig:      lokiConfigIn,
		timestampScale: float64(timestampScale),
		saneLabels:     saneLabels,
		client:         client,
		timeNow:        time.Now,
		exitChan:       pUtils.ExitChannel(),
		in:             in,
		metrics:        newMetrics(opMetrics, params.Name),
	}

	go l.processRecords()

	return l, nil
}
