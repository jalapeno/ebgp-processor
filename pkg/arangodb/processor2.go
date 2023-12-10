package arangodb

import (
	"context"
	"strconv"
	"strings"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) processebgpPeer(ctx context.Context, key, id string, e message.PeerStateChange) error {
	if e.RemoteASN == e.LocalASN {
		glog.Infof("ibgp peer, no processing needed: %+v", e.Key)
		return nil
	} else {

		obj := ebgpPeer{
			Key:             e.RemoteBGPID + "_" + strconv.Itoa(int(e.RemoteASN)),
			BGPRouterID:     e.RemoteBGPID,
			ASN:             int32(e.RemoteASN),
			AdvCapabilities: e.AdvCapabilities,
		}

		if !strings.Contains(key, ":") {
			if _, err := a.ebgpPeerV4.CreateDocument(ctx, &obj); err != nil {
				glog.Infof("create v4 peer: %+v", e.Key)
				if !driver.IsConflict(err) {
					return err
				}
			}
			if _, err := a.ebgpSessionV4.CreateDocument(ctx, &e); err != nil {
				glog.Infof("create v4 session: %+v", e.Key)
				if !driver.IsConflict(err) {
					return err
				}
			}

		} else {
			//if strings.Contains(key, ":") {
			if _, err := a.ebgpPeerV6.CreateDocument(ctx, &obj); err != nil {
				glog.Infof("create v6 peer: %+v", e.Key)
				if !driver.IsConflict(err) {
					return err
				}

				if _, err := a.ebgpSessionV6.CreateDocument(ctx, &e); err != nil {
					glog.Infof("create v6 session: %+v", e.Key)
					if !driver.IsConflict(err) {
						return err
					}
				}
			}
			//}
		}
		if err := a.dedupeSession(); err != nil {
			if err != nil {
				return err
			}
		}

	}
	return nil
}

func (a *arangoDB) dedupeSession() error {
	ctx := context.TODO()
	dup_query := "for l in ebgp_session_v4 filter l._key like " + "\"%:%\"" +
		" remove l in ebgp_session_v4"
	pcursor, err := a.db.Query(ctx, dup_query, nil)
	glog.Infof("dedup query: %+v", dup_query)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	return nil
}

// process Removal removes records from the inetprefixV4 collection
func (a *arangoDB) processPeerSessionRemoval(ctx context.Context, key string, e *message.PeerStateChange) error {
	if strings.Contains(key, ":") {
		// glog.Infof("removing v6 peer %+v", key)
		// query := "for d in ebgp_session_v6" +
		// 	" filter d._key == " + "\"" + key + "\""
		// query += " return d"

		glog.Infof("removing v6 peer %+v", key)
		query := "for d in ebgp_session_v6" +
			" filter d._key == " + "\"" + key + "\""
		query += " remove d in ebgp_session_v6"
		glog.Infof("query: %+v", query)
		ncursor, err := a.db.Query(ctx, query, nil)
		if err != nil {
			return err
		}
		defer ncursor.Close()

		// for {
		// 	var nm message.PeerStateChange
		// 	m, err := ncursor.ReadDocument(ctx, &nm)
		// 	if err != nil {
		// 		if !driver.IsNoMoreDocuments(err) {
		// 			return err
		// 		}
		// 		break
		// 	}
		// 	//if _, err := a.ebgpSessionV6.RemoveDocument(ctx, m.ID.Key()); err != nil {
		// 	if _, err := a.ebgpSessionV6.RemoveDocument(ctx, key); err != nil {
		// 		glog.Infof("remove v6 ebgp session: %+v, doc: %+v, session: %+v", key, m, a.ebgpSessionV6)
		// 		if !driver.IsNotFound(err) {
		// 			return err
		// 		}
		// 	}
		// }
	} else {
		glog.Infof("removing v4 ebgp session %+v", e.Key)
		query := "for d in " + a.ebgpSessionV4.Name() +
			" filter d._key == " + "\"" + key + "\""
		query += " return d"
		//glog.Infof("query: %+v", query)
		ncursor, err := a.db.Query(ctx, query, nil)
		if err != nil {
			return err
		}
		defer ncursor.Close()

		for {
			var nm message.PeerStateChange
			m, err := ncursor.ReadDocument(ctx, &nm)
			if err != nil {
				if !driver.IsNoMoreDocuments(err) {
					return err
				}
				break
			}
			if _, err := a.ebgpSessionV4.RemoveDocument(ctx, m.ID.Key()); err != nil {
				glog.Infof("remove v4 bgp session: %+v", key)
				if !driver.IsNotFound(err) {
					return err
				}
			}
		}

	}
	return nil
}

func (a *arangoDB) processInet4(ctx context.Context, key, id string, e message.UnicastPrefix) error {
	// get internal ASN so we can determine whether this is an external prefix or not
	getasn := "for l in ls_node_extended limit 1 return l"
	cursor, err := a.db.Query(ctx, getasn, nil)
	if err != nil {
		return err
	}
	var ln LSNodeExt
	lm, err := cursor.ReadDocument(ctx, &ln)
	glog.V(5).Infof("meta %+v", lm)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}

	var result bool = false
	for _, x := range e.BaseAttributes.ASPath {
		if x == uint32(ln.ASN) {
			result = true
			break
		}
	}
	if result {
		glog.V(5).Infof("internal ASN %+v found in unicast prefix, do not process", e.Prefix)
	}

	if e.OriginAS == ln.ASN {
		glog.V(5).Infof("internal prefix, do not process: %+v, origin_as: %+v, ln.ASN: %+v", e.Prefix, e.OriginAS, ln.ASN)
		return a.processInet4Removal(ctx, key, &e)

	} else {
		obj := inetPrefix{
			//Key: inetKey,
			Key:       e.Prefix + "_" + strconv.Itoa(int(e.PrefixLen)),
			Prefix:    e.Prefix,
			PrefixLen: e.PrefixLen,
			OriginAS:  e.OriginAS,
			NextHop:   e.Nexthop,
		}
		if _, err := a.inetprefixV4.CreateDocument(ctx, &obj); err != nil {
			//glog.Infof("adding prefix: %+v", e.Prefix+"_"+strconv.Itoa(int(e.PrefixLen)))
			if !driver.IsConflict(err) {
				return nil
			}
		}
		// The document already exists, updating it with the latest info
		if _, err := a.inetprefixV4.UpdateDocument(ctx, ln.Key, &obj); err != nil {
			if !driver.IsConflict(err) {
				return nil
			}
		}
	}
	return nil
}

func (a *arangoDB) processebgp4(ctx context.Context, key, id string, e message.UnicastPrefix) error {
	// get internal ASN so we can determine whether this is an external prefix or not
	getasn := "for l in ls_node_extended limit 1 return l"
	cursor, err := a.db.Query(ctx, getasn, nil)
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

	if e.OriginAS == ln.ASN {
		glog.V(5).Infof("internal prefix, do not process: %+v, origin_as: %+v, ln.ASN: %+v", e.Prefix, e.OriginAS, ln.ASN)
		return a.processInet4Removal(ctx, key, &e)

	} else {
		obj := inetPrefix{
			//Key: inetKey,
			Key:       e.Prefix + "_" + strconv.Itoa(int(e.PrefixLen)),
			Prefix:    e.Prefix,
			PrefixLen: e.PrefixLen,
			OriginAS:  e.OriginAS,
			NextHop:   e.Nexthop,
		}
		if _, err := a.inetprefixV4.CreateDocument(ctx, &obj); err != nil {
			//glog.Infof("adding prefix: %+v", e.Prefix+"_"+strconv.Itoa(int(e.PrefixLen)))
			if !driver.IsConflict(err) {
				return nil
			}
		}
		// The document already exists, updating it with the latest info
		if _, err := a.inetprefixV4.UpdateDocument(ctx, ln.Key, &obj); err != nil {
			if !driver.IsConflict(err) {
				return nil
			}
		}
	}
	return nil
}

// process Removal removes records from the inetprefixV4 collection
func (a *arangoDB) processInet4Removal(ctx context.Context, key string, e *message.UnicastPrefix) error {
	query := "for d in " + a.inetprefixV4.Name() +
		" filter d.prefix == " + "\"" + e.Prefix + "\""
	query += " return d"
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()

	for {
		var nm inetPrefix
		m, err := ncursor.ReadDocument(ctx, &nm)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		if _, err := a.inetprefixV4.RemoveDocument(ctx, m.ID.Key()); err != nil {
			if !driver.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func (a *arangoDB) processInet6(ctx context.Context, key, id string, e message.UnicastPrefix) error {
	// get internal ASN so we can determine whether this is an external prefix or not
	getasn := "for l in ls_node_extended limit 1 return l"
	cursor, err := a.db.Query(ctx, getasn, nil)
	if err != nil {
		return err
	}

	var ln LSNodeExt
	lm, err := cursor.ReadDocument(ctx, &ln)
	glog.V(5).Infof("meta %+v", lm)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}

	var result bool = false
	for _, x := range e.BaseAttributes.ASPath {
		if x == uint32(ln.ASN) {
			result = true
			break
		}
	}
	if result {
		glog.V(5).Infof("internal ASN %+v found in unicast prefix, do not process", e.Prefix)
	}

	//glog.Infof("got message %+v", &e)
	if e.OriginAS == ln.ASN {
		//glog.Infof("internal prefix, do not process: %+v, origin_as: %+v, ln.ASN: %+v", e.Prefix, e.OriginAS, ln.ASN)
		return a.processInet6Removal(ctx, key, &e)
	}
	if e.OriginAS == 0 {
		//glog.Infof("internal prefix, do not process: %+v, origin_as: %+v, ln.ASN: %+v", e.Prefix, e.OriginAS, ln.ASN)
		return a.processInet4Removal(ctx, key, &e)

	} else {
		obj := inetPrefix{
			Key:       e.Prefix + "_" + strconv.Itoa(int(e.PrefixLen)),
			Prefix:    e.Prefix,
			PrefixLen: e.PrefixLen,
			OriginAS:  e.OriginAS,
			NextHop:   e.Nexthop,
		}
		if _, err := a.inetprefixV6.CreateDocument(ctx, &obj); err != nil {
			//glog.Infof("adding prefix: %+v", e.Prefix+"_"+strconv.Itoa(int(e.PrefixLen)))
			if !driver.IsConflict(err) {
				return nil
			}
		}
		// The document already exists, updating it with the latest info
		if _, err := a.inetprefixV6.UpdateDocument(ctx, ln.Key, &obj); err != nil {
			if !driver.IsConflict(err) {
				return nil
			}
		}
	}
	return nil
}

// process Removal removes records from the inetprefixV4 collection
func (a *arangoDB) processInet6Removal(ctx context.Context, key string, e *message.UnicastPrefix) error {
	query := "for d in " + a.inetprefixV6.Name() +
		" filter d._key == " + "\"" + key + "\""
	query += " return d"
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()

	for {
		var nm inetPrefix
		m, err := ncursor.ReadDocument(ctx, &nm)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		if _, err := a.inetprefixV6.RemoveDocument(ctx, m.ID.Key()); err != nil {
			if !driver.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}
