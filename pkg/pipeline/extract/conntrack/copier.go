package conntrack

import (
	"fmt"

	"github.com/netobserv/flowlogs-pipeline/pkg/api"
	"github.com/netobserv/flowlogs-pipeline/pkg/config"
)

type copier interface {
	// addField adds an copy field to the connection
	addField(conn connection)
	// update updates the field value in the connection based on the flow log.
	update(conn connection, flowLog config.GenericMap, isNew bool)
}

type copyBase struct {
	inputField  string
	outputField string
}

type cFirst struct{ copyBase }
type cLast struct{ copyBase }

// newCopier returns a new copier depending on the output field operation
func newCopier(of api.OutputField) (copier, error) {
	if of.Name == "" {
		return nil, fmt.Errorf("empty name %v", of)
	}
	var inputField string
	if of.Input != "" {
		inputField = of.Input
	} else {
		inputField = of.Name
	}
	cBase := copyBase{inputField: inputField, outputField: of.Name}
	var c copier
	switch of.Operation {
	case api.ConnTrackOperationName("CopyFirst"):
		c = &cFirst{cBase}
	case api.ConnTrackOperationName("CopyLast"):
		c = &cLast{cBase}
	default:
		return nil, fmt.Errorf("unknown operation: %q", of.Operation)
	}
	return c, nil
}

func (cp *copyBase) addField(conn connection) {
	conn.addCp(cp.outputField)
}

func (cp *cFirst) update(conn connection, flowLog config.GenericMap, isNew bool) {
	if isNew {
		conn.updateCpValue(cp.outputField, flowLog[cp.inputField])
	}
}

func (cp *cLast) update(conn connection, flowLog config.GenericMap, isNew bool) {
	conn.updateCpValue(cp.outputField, flowLog[cp.inputField])
}
