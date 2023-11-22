package arangodb

import (
	"context"
	"encoding/json"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/jalapeno/topology/pkg/dbclient"
	notifier "github.com/jalapeno/topology/pkg/kafkanotifier"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/tools"
)

type arangoDB struct {
	dbclient.DB
	*ArangoConn
	stop            chan struct{}
	peer            driver.Collection
	ebgpPeerV4      driver.Collection
	ebgpPeerV6      driver.Collection
	unicastprefixV4 driver.Collection
	unicastprefixV6 driver.Collection
	inetprefixV4    driver.Collection
	inetprefixV6    driver.Collection
}

// NewDBSrvClient returns an instance of a DB server client process
func NewDBSrvClient(arangoSrv, user, pass, dbname, peer, ebgpPeerV4 string, ebgpPeerV6 string, unicastprefixV4, unicastprefixV6,
	inetprefixV4 string, inetprefixV6 string) (dbclient.Srv, error) {
	if err := tools.URLAddrValidation(arangoSrv); err != nil {
		return nil, err
	}
	arangoConn, err := NewArango(ArangoConfig{
		URL:      arangoSrv,
		User:     user,
		Password: pass,
		Database: dbname,
	})
	if err != nil {
		return nil, err
	}
	arango := &arangoDB{
		stop: make(chan struct{}),
	}
	arango.DB = arango
	arango.ArangoConn = arangoConn

	// Check if peer collection exists, if not fail as Jalapeno topology is not running
	arango.peer, err = arango.db.Collection(context.TODO(), peer)
	if err != nil {
		return nil, err
	}
	// Check if unicast_prefix_v4 collection exists, if not fail as Jalapeno topology is not running
	arango.unicastprefixV4, err = arango.db.Collection(context.TODO(), unicastprefixV4)
	if err != nil {
		return nil, err
	}
	// Check if unicast_prefix_v4 collection exists, if not fail as Jalapeno ipv4_topology is not running
	arango.unicastprefixV6, err = arango.db.Collection(context.TODO(), unicastprefixV6)
	if err != nil {
		return nil, err
	}

	// check for ebgp_peer_v4 collection
	found, err := arango.db.CollectionExists(context.TODO(), ebgpPeerV4)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), ebgpPeerV4)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// check for ebgp_peer_v6 collection
	found, err = arango.db.CollectionExists(context.TODO(), ebgpPeerV6)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), ebgpPeerV6)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// check for inet4 collection
	found, err = arango.db.CollectionExists(context.TODO(), inetprefixV4)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), inetprefixV4)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}
	// check for inet6 collection
	found, err = arango.db.CollectionExists(context.TODO(), inetprefixV6)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), inetprefixV6)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// create ebgp_peer_v4 edge collection
	var ebgpPeerV4_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.ebgpPeerV4, err = arango.db.CreateCollection(context.TODO(), "ebgp_peer_v4", ebgpPeerV4_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.ebgpPeerV4, err = arango.db.Collection(context.TODO(), ebgpPeerV4)
	if err != nil {
		return nil, err
	}

	// create ebgp_peer_v4 edge collection
	var ebgpPeerV6_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.ebgpPeerV6, err = arango.db.CreateCollection(context.TODO(), "ebgp_peer_v6", ebgpPeerV6_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.ebgpPeerV4, err = arango.db.Collection(context.TODO(), ebgpPeerV4)
	if err != nil {
		return nil, err
	}

	// create inet prefix V4 edge collection
	var inetV4_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.inetprefixV4, err = arango.db.CreateCollection(context.TODO(), "inet_prefix_v4", inetV4_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.inetprefixV4, err = arango.db.Collection(context.TODO(), inetprefixV4)
	if err != nil {
		return nil, err
	}

	// create unicast prefix V6 edge collection
	var inetV6_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.inetprefixV6, err = arango.db.CreateCollection(context.TODO(), "inet_prefix_v6", inetV6_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.inetprefixV6, err = arango.db.Collection(context.TODO(), inetprefixV6)
	if err != nil {
		return nil, err
	}
	return arango, nil
}

func (a *arangoDB) Start() error {
	if err := a.loadCollection(); err != nil {
		return err
	}
	glog.Infof("Connected to arango database, starting monitor")
	go a.monitor()

	return nil
}

func (a *arangoDB) Stop() error {
	close(a.stop)

	return nil
}

func (a *arangoDB) GetInterface() dbclient.DB {
	return a.DB
}

func (a *arangoDB) GetArangoDBInterface() *ArangoConn {
	return a.ArangoConn
}

func (a *arangoDB) StoreMessage(msgType dbclient.CollectionType, msg []byte) error {
	event := &notifier.EventMessage{}
	if err := json.Unmarshal(msg, event); err != nil {
		return err
	}
	event.TopicType = msgType
	switch msgType {
	// case bmp.PeerStateChangeMsg:
	// 	return a.peerHandler(event)
	case bmp.UnicastPrefixV4Msg:
		return a.unicastV4Handler(event)
	case bmp.UnicastPrefixV6Msg:
		return a.unicastV6Handler(event)
	}
	return nil
}

func (a *arangoDB) monitor() {
	for {
		select {
		case <-a.stop:
			return
		}
	}
}

func (a *arangoDB) loadCollection() error {
	ctx := context.TODO()

	glog.Infof("copying ipv4 eBGP peers to ebgp_peer_v4 collection")
	peer_query := "for l in peer filter l._key !like " + "\"%:%\"" +
		"filter l.remote_asn != l.local_asn insert l in ebgp_peer_v4"
	cursor, err := a.db.Query(ctx, peer_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// remove internal peer entries from collection
	glog.Infof("removing any internal v4 peers that got copied over to ebgp_peer_v4 collection")
	dup_query := "LET duplicates = ( FOR d IN ls_node_extended COLLECT asn = d.asn WITH COUNT INTO count " +
		"FILTER count > 1 RETURN { asn: asn, count: count }) " +
		"FOR d IN duplicates FOR m IN ebgp_peer_v4 FILTER d.asn == m.remote_asn remove m in ebgp_peer_v4"
	pcursor, err := a.db.Query(ctx, dup_query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()

	glog.Infof("copying ipv6 eBGP peers to ebgp_peer_v4 collection")
	peerv6_query := "for l in peer filter l._key like " + "\"%:%\"" +
		"filter l.remote_asn != l.local_asn insert l in ebgp_peer_v6"
	cursor, err = a.db.Query(ctx, peerv6_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// remove internal peer entries from  collection
	glog.Infof("removing any internal v6 peers that got copied over to ebgp_peer_v4 collection")
	dup_query = "LET duplicates = ( FOR d IN ls_node_extended COLLECT asn = d.asn WITH COUNT INTO count " +
		"FILTER count > 1 RETURN { asn: asn, count: count }) " +
		"FOR d IN duplicates FOR m IN ebgp_peer_v6 FILTER d.asn == m.remote_asn remove m in ebgp_peer_v6"
	pcursor, err = a.db.Query(ctx, dup_query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()

	glog.Infof("copying unicast v4 prefixes into inet_prefix_v4 collection")
	prefix_query := "for l in unicast_prefix_v4 filter l.prefix_len < 26 " +
		"filter l.remote_asn != l.origin_as filter l.base_attrs.local_pref == null " +
		"INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", l.prefix, l.prefix_len), prefix: l.prefix, prefix_len: l.prefix_len, " +
		"origin_as: l.origin_as, nexthop: l.nexthop } INTO inet_prefix_v4 OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// remove internal prefix entries from collection
	glog.Infof("removing any internal v4 prefixes that got copied over to inet_prefix_v4 collection")
	dup_query = "LET duplicates = ( FOR d IN ls_node_extended COLLECT asn = d.asn WITH COUNT INTO count " +
		"FILTER count > 1 RETURN { asn: asn, count: count }) " +
		"FOR d IN duplicates FOR m IN inet_prefix_v4 FILTER m.prefix != " + "\"0.0.0.0\"" +
		" filter d.asn == m.origin_as remove m in inet_prefix_v4"
	pcursor, err = a.db.Query(ctx, dup_query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()

	glog.Infof("copying unicast v6 prefixes into inet_prefix_v6 collection")
	prefix6_query := "for l in unicast_prefix_v6 filter l.prefix_len < 80 " +
		"filter l.remote_asn != l.origin_as filter l.base_attrs.local_pref == null " +
		"INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", l.prefix, l.prefix_len), prefix: l.prefix, prefix_len: l.prefix_len, " +
		"origin_as: l.origin_as, nexthop: l.nexthop } INTO inet_prefix_v6 OPTIONS { overwrite: true }"
	cursor, err = a.db.Query(ctx, prefix6_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// remove internal prefix entries from collection
	glog.Infof("removing any internal v6 prefixes that got copied over to inet_prefix_v6 collection")
	dup_query = "LET external = ( FOR d IN ls_node_extended COLLECT asn = d.asn WITH COUNT INTO count " +
		"FILTER count > 1 RETURN { asn: asn, count: count }) " +
		"FOR d IN external FOR m IN inet_prefix_v6 filter m.prefix_len < 96 filter m.remote_asn != m.origin_as " +
		"filter m.prefix != " + "\"::\" filter d.asn == m.origin_as remove m in inet_prefix_v6"
	pcursor, err = a.db.Query(ctx, dup_query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()

	return nil
}
