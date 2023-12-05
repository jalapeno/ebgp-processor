package arangodb

import (
	"context"
	"encoding/json"
	"strconv"

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
	ebgpSessionV4   driver.Collection
	ebgpSessionV6   driver.Collection
	unicastprefixV4 driver.Collection
	unicastprefixV6 driver.Collection
	ebgpprefixV4    driver.Collection
	ebgpprefixV6    driver.Collection
	inetprefixV4    driver.Collection
	inetprefixV6    driver.Collection
}

// NewDBSrvClient returns an instance of a DB server client process
func NewDBSrvClient(arangoSrv, user, pass, dbname, peer, ebgpPeerV4 string, ebgpPeerV6 string, ebgpSessionV4 string,
	ebgpSessionV6 string, unicastprefixV4, unicastprefixV6, ebgpprefixV4, ebgpprefixV6,
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

	// check for ebgp_session_v4 collection
	found, err = arango.db.CollectionExists(context.TODO(), ebgpSessionV4)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), ebgpSessionV4)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// check for ebgp_session_v6 collection
	found, err = arango.db.CollectionExists(context.TODO(), ebgpSessionV6)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), ebgpSessionV6)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// check for ebgp4 prefix collection
	found, err = arango.db.CollectionExists(context.TODO(), ebgpprefixV4)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), ebgpprefixV4)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}
	// check for ebgp6 prefix collection
	found, err = arango.db.CollectionExists(context.TODO(), ebgpprefixV6)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), ebgpprefixV6)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// check for inet4 prefix collection
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
	// check for inet6 prefix collection
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

	// create ebgp_peer_v4 collection
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

	// create ebgp_peer_v6 collection
	var ebgpPeerV6_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.ebgpPeerV6, err = arango.db.CreateCollection(context.TODO(), "ebgp_peer_v6", ebgpPeerV6_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.ebgpPeerV6, err = arango.db.Collection(context.TODO(), ebgpPeerV6)
	if err != nil {
		return nil, err
	}

	// create ebgp_session_v4 collection
	var ebgpSessionV4_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.ebgpSessionV4, err = arango.db.CreateCollection(context.TODO(), "ebgp_session_v4", ebgpSessionV4_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.ebgpSessionV4, err = arango.db.Collection(context.TODO(), ebgpSessionV4)
	if err != nil {
		return nil, err
	}

	// create ebgp_session_v6 collection
	var ebgpSessionV6_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.ebgpSessionV6, err = arango.db.CreateCollection(context.TODO(), "ebgp_session_v6", ebgpSessionV6_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.ebgpSessionV6, err = arango.db.Collection(context.TODO(), ebgpSessionV4)
	if err != nil {
		return nil, err
	}

	// create ebgp prefix V4 collection
	var ebgpprefixV4_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.ebgpprefixV4, err = arango.db.CreateCollection(context.TODO(), "ebgp_prefix_v4", ebgpprefixV4_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.ebgpprefixV4, err = arango.db.Collection(context.TODO(), ebgpprefixV4)
	if err != nil {
		return nil, err
	}

	// create ebgp prefix V6 collection
	var ebgpprefixV6_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.ebgpprefixV6, err = arango.db.CreateCollection(context.TODO(), "ebgp_prefix_v6", ebgpprefixV6_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.ebgpprefixV6, err = arango.db.Collection(context.TODO(), ebgpprefixV6)
	if err != nil {
		return nil, err
	}

	// create inet prefix V4 collection
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

	// create unicast prefix V6 collection
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
	case bmp.PeerStateChangeMsg:
		return a.peerHandler(event)
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

	// Establish internal ipv4 eBGP peering sessions
	glog.Infof("copying ipv4 eBGP peer sessions to ebgp_session_v4 collection")
	ebgp4session_query := "for l in peer filter l._key !like " + "\"%:%\"" +
		"filter l.remote_asn != l.local_asn insert l in ebgp_session_v4"
	cursor, err := a.db.Query(ctx, ebgp4session_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// remove iBGP sessions from collection
	glog.Infof("removing any internal v4 sessions that got copied over to ebgp_session_v4 collection")
	dup4_query := "LET duplicates = ( FOR d IN ls_node_extended COLLECT asn = d.asn WITH COUNT INTO count " +
		"FILTER count > 1 RETURN { asn: asn, count: count }) " +
		"FOR d IN duplicates FOR m IN ebgp_session_v4 FILTER d.asn == m.remote_asn remove m in ebgp_session_v4"
	pcursor, err := a.db.Query(ctx, dup4_query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()

	// Establish internal ipv6 eBGP peering sessions
	glog.Infof("copying ipv6 eBGP v6 sessions to ebgp_session_v6 collection")
	ebgp6session_query := "for l in peer filter l._key like " + "\"%:%\"" +
		"filter l.remote_asn != l.local_asn insert l in ebgp_session_v6"
	cursor, err = a.db.Query(ctx, ebgp6session_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// remove ibgp sessions from  collection
	glog.Infof("removing any internal v6 sessions that got copied over to ebgp_session_v4 collection")
	dup6_query := "LET duplicates = ( FOR d IN ls_node_extended COLLECT asn = d.asn WITH COUNT INTO count " +
		"FILTER count > 1 RETURN { asn: asn, count: count }) " +
		"FOR d IN duplicates FOR m IN ebgp_session_v6 FILTER d.asn == m.remote_asn remove m in ebgp_session_v6"
	pcursor, err = a.db.Query(ctx, dup6_query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()

	// Identify unique ipv4 eBGP peers
	glog.Infof("copying unique ebgp peers into ebgp_peer_v4 collection")
	ebgp4peer_query := "for l in ebgp_session_v4 " +
		"INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", l.remote_bgp_id, l.remote_asn), bgp_router_id: l.remote_bgp_id, " +
		"asn: l.remote_asn, adv_cap: l.adv_cap } INTO ebgp_peer_v4 OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, ebgp4peer_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// Identify unique ipv6 eBGP peers
	glog.Infof("copying unique ebgp peers into ebgp_peer_v6 collection")
	ebgp6peer_query := "for l in ebgp_session_v6 " +
		"INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", l.remote_bgp_id, l.remote_asn), bgp_router_id: l.remote_bgp_id, " +
		"asn: l.remote_asn, adv_cap: l.adv_cap } INTO ebgp_peer_v6 OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, ebgp6peer_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// get internal ASN so we can determine whether this is an external prefix or not
	getasn := "for l in ls_node_extended limit 1 return l"
	cursor, err = a.db.Query(ctx, getasn, nil)
	if err != nil {
		return err
	}

	var ln LSNodeExt
	lm, err := cursor.ReadDocument(ctx, &ln)
	glog.Infof("meta %+v", lm)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}
	internalASN := strconv.Itoa(int(ln.ASN))

	glog.Infof("copying unicast v4 prefixes into ebgp_prefix_v4 collection")
	ebgp4_query := "for l in unicast_prefix_v4 filter l.origin_as in 64512..65535 filter l.prefix_len < 26 " +
		"filter l.remote_asn != l.origin_as filter l.base_attrs.local_pref == null " +
		"INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", l.prefix, l.prefix_len), prefix: l.prefix, prefix_len: l.prefix_len, " +
		"origin_as: l.origin_as, nexthop: l.nexthop } INTO ebgp_prefix_v4 OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, ebgp4_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	glog.Infof("copying unicast v4 prefixes into inet_prefix_v4 collection")
	inet4_query := "for l in unicast_prefix_v4 filter l.peer_asn !in 64512..65535 filter l.prefix_len < 26 " +
		"filter l.remote_asn != l.origin_as filter l.base_attrs.local_pref == null " +
		"filter l.base_attrs.as_path not like " + "\"%" + internalASN + "%\"" +
		"INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", l.prefix, l.prefix_len), prefix: l.prefix, prefix_len: l.prefix_len, " +
		"origin_as: l.origin_as, nexthop: l.nexthop } INTO inet_prefix_v4 OPTIONS { ignoreErrors: true }"
	cursor, err = a.db.Query(ctx, inet4_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	glog.Infof("copying ebgp unicast v6 prefixes into ebgp_prefix_v6 collection")
	ebgp6_query := "for l in unicast_prefix_v6 filter l.origin_as in 64512..65535 filter l.prefix_len < 80 " +
		"filter l.remote_asn != l.origin_as filter l.base_attrs.local_pref == null " +
		"INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", l.prefix, l.prefix_len), prefix: l.prefix, prefix_len: l.prefix_len, " +
		"origin_as: l.origin_as, nexthop: l.nexthop } INTO ebgp_prefix_v6 OPTIONS { overwrite: true }"
	cursor, err = a.db.Query(ctx, ebgp6_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	glog.Infof("copying internet unicast v6 prefixes into inet_prefix_v6 collection")
	inet6_query := "for l in unicast_prefix_v6 filter l.peer_asn !in 64512..65535 filter l.prefix_len < 80 " +
		"filter l.remote_asn != l.origin_as filter l.base_attrs.local_pref == null " +
		"filter l.base_attrs.as_path not like " + "\"%" + internalASN + "%\"" +
		" INSERT { _key: CONCAT_SEPARATOR(" + "\"_\", l.prefix, l.prefix_len), prefix: l.prefix, prefix_len: l.prefix_len, " +
		"origin_as: l.origin_as, nexthop: l.nexthop } INTO inet_prefix_v6 OPTIONS { overwrite: true }"
	cursor, err = a.db.Query(ctx, inet6_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	return nil
}
