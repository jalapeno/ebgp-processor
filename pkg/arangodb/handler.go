package arangodb

import (
	"context"
	"fmt"
	"strings"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	notifier "github.com/jalapeno/topology/pkg/kafkanotifier"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) peerHandler(obj *notifier.EventMessage) error {
	ctx := context.TODO()
	//glog.Infof("handler obj: %+v", obj)
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}
	// Check if Collection encoded in ID exists
	c := strings.Split(obj.ID, "/")[0]
	if strings.Compare(c, a.peer.Name()) != 0 {
		return fmt.Errorf("configured collection name %s and received in event collection name %s do not match", a.peer.Name(), c)
	}
	glog.Infof("Processing Peer action: %s for ID: %s, Key: %s", obj.Action, obj.ID, obj.Key)
	var o message.PeerStateChange
	_, err := a.peer.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a prefix removal notification, reading it will return Not Found error
		if !driver.IsNotFound(err) {
			return fmt.Errorf("failed to read existing document %s with error: %+v", obj.Key, err)
		}
		// If operation matches to "down" then it is confirmed delete operation, otherwise return error
		if obj.Action != "down" {
			return fmt.Errorf("document %s not found but Action is not \"del\", possible stale event", obj.Key)
		}
		glog.Infof("del action %+v", obj)
		//return nil //a.processPeerSessionRemoval(ctx, obj.Key, &o)
	}
	switch obj.Action {
	case "add":
		fallthrough
	case "update":
		//glog.Infof("adding %+v, obj: %+v", obj.Key, &o)
		if err := a.processBgpNode(ctx, obj.Key, obj.ID, o); err != nil {
			return fmt.Errorf("failed to process action %s for vertex %s with error: %+v", obj.Action, obj.Key, err)
		}
	case "down":
		if err := a.processPeerSessionRemoval(ctx, obj.Key, &o); err != nil {
			return fmt.Errorf("failed to process action %s for vertex %s with error: %+v", obj.Action, obj.Key, err)
		}
	default:
	}
	return nil
}

func (a *arangoDB) unicastV4Handler(obj *notifier.EventMessage) error {
	ctx := context.TODO()
	//glog.Infof("handler obj: %+v", obj)
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}

	// Check if Collection encoded in ID exists
	c := strings.Split(obj.ID, "/")[0]
	if strings.Compare(c, a.unicastprefixV4.Name()) != 0 {
		return fmt.Errorf("configured collection name %s and received in event collection name %s do not match", a.unicastprefixV4.Name(), c)
	}
	//glog.Infof("Processing unicast prefix action: %s for key: %s ID: %s", obj.Action, obj.Key, obj.ID)
	var o message.UnicastPrefix
	_, err := a.unicastprefixV4.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a prefix removal notification, reading it will return Not Found error
		if !driver.IsNotFound(err) {
			return fmt.Errorf("failed to read existing document %s with error: %+v", obj.Key, err)
		}
		// If operation matches to "del" then it is confirmed delete operation, otherwise return error
		if obj.Action != "del" {
			return fmt.Errorf("document %s not found but Action is not \"del\", possible stale event", obj.Key)
		}
		return a.processInet4Removal(ctx, obj.Key, &o)
	}
	switch obj.Action {
	case "add":
		fallthrough
	case "update":
		//glog.Infof("passing to processInet: %+v", o)
		if err := a.processInet4(ctx, obj.Key, obj.ID, o); err != nil {
			return fmt.Errorf("failed to process action %s for vertex %s with error: %+v", obj.Action, obj.Key, err)
		}
	default:
	}
	return nil
}

func (a *arangoDB) unicastV6Handler(obj *notifier.EventMessage) error {
	ctx := context.TODO()
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}
	// Check if Collection encoded in ID exists
	c := strings.Split(obj.ID, "/")[0]
	if strings.Compare(c, a.unicastprefixV6.Name()) != 0 {
		return fmt.Errorf("configured collection name %s and received in event collection name %s do not match", a.unicastprefixV6.Name(), c)
	}
	glog.V(6).Infof("Processing action: %s for key: %s ID: %s", obj.Action, obj.Key, obj.ID)
	var o message.UnicastPrefix
	_, err := a.unicastprefixV6.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a prefix removal notification, reading it will return Not Found error
		if !driver.IsNotFound(err) {
			return fmt.Errorf("failed to read existing document %s with error: %+v", obj.Key, err)
		}
		// If operation matches to "del" then it is confirmed delete operation, otherwise return error
		if obj.Action != "del" {
			return fmt.Errorf("document %s not found but Action is not \"del\", possible stale event", obj.Key)
		}
		return a.processInet6Removal(ctx, obj.Key, &o)
	}
	switch obj.Action {
	case "add":
		fallthrough
	case "update":
		if err := a.processInet6(ctx, obj.Key, obj.ID, o); err != nil {
			return fmt.Errorf("failed to process action %s for vertex %s with error: %+v", obj.Action, obj.Key, err)
		}
	default:
	}
	return nil
}
