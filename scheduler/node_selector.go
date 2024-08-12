/*
	Copyright (C) CESS. All rights reserved.
	Copyright (C) Cumulus Encrypted Storage System. All rights reserved.

	SPDX-License-Identifier: Apache-2.0

    This package aims to provide CESS developers with an integrated, customizable user data scheduling module
	(that is, distributing user data to nearby nodes with good communication conditions).
	This package contains a node selection module and a data scheduling module built on top of it,
	which can be used by developers respectively.
*/

package scheduler

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CESSProject/cess-go-tools/utils"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/mr-tron/base58/base58"
	"github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
)

const (
	FIXED_STRATEGY       = "fixed"
	PRIORITY_STRATEGY    = "priority"
	DEFAULT_STRATEGY     = "priority"
	MAX_ALLOWED_NODES    = 120
	MAX_DISALLOWED_NODES = 1024
	DEFAULT_MAX_TTL      = time.Millisecond * 500
	DEFAULT_TIMEOUT      = 5 * time.Second
	DEFAULT_FLUSH_TIME   = time.Hour * 4
	PEER_NODES_GAP       = time.Microsecond * 25
	MAX_FAILED_CONN      = 3
	NODE_LIST_TEMP       = `{
		"allowed_peers":{
			"12D3KooWS3MWZZmRuejE2t2oJKQ9eNHxfnqw6EVxRRtYV3XPQkHy":"/ip4/127.0.0.1/tcp/4001" 
		},
		"disallowed_peers":[
			"12D3KooWS3MWZZmRuejE2t2oJKQ9eNHxfnqw6EVxRRtYV3XPQkHy"
		]
	}`
)

type Selector interface {
	NewPeersIterator(minNum int) (Iterator, error)
	Feedback(id string, isWork bool)
	FlushPeerNodes(pingTimeout time.Duration, peers ...peer.AddrInfo)
	GetPeersNumber() int
	ClearBlackList()
	FlushlistedPeerNodes(pingTimeout time.Duration, discoverer Discoverer)
	NewPeersIteratorWithConditions(minNum, maxNum int, conds ...func(key string, value NodeInfo) bool) (Iterator, error)
	NewPeersIteratorWithListPeers(minNum int) (Iterator, error)
	NewPeersIteratorWithActivePeers(minNum int) (Iterator, error)
}

type Iterator interface {
	GetPeer() (peer.AddrInfo, bool)
}

type Discoverer interface {
	FindPeer(ctx context.Context, id peer.ID) (pi peer.AddrInfo, err error)
}

type NodeList struct {
	AllowedPeers    map[string]string `json:"allowed_peers"` // k=peerId; value=muti-address, if null, autocomplete
	DisallowedPeers []string          `json:"disallowed_peers"`
}

type SelectorConfig struct {
	Strategy      string `name:"Strategy" toml:"Strategy" yaml:"Strategy"` //case in "fixed","priority", default: "priority"
	NodeFilePath  string `name:"NodeFile" toml:"NodeFile" yaml:"NodeFile"`
	MaxNodeNum    int    `name:"MaxNodeNum" toml:"MaxNodeNum" yaml:"MaxNodeNum"`
	MaxTTL        int64  `name:"MaxTTL" toml:"MaxTTL" yaml:"MaxTTL"`                      // unit: millisecond, default: 300 ms
	FlushInterval int64  `name:"FlushInterval" toml:"FlushInterval" yaml:"FlushInterval"` // unit: hours, default: 4h
}

type NodeInfo struct {
	NePoints  int
	Available bool
	AddrInfo  peer.AddrInfo
	TTL       time.Duration
	FlushTime time.Time
}

type NodeMap struct {
	nodes map[string]peer.AddrInfo
	count int
}

type NodeSelector struct {
	listPeers   *sync.Map
	blackList   *bloom.BloomFilter
	activePeers *sync.Map
	config      SelectorConfig
	peerNum     *atomic.Int32
	lastOne     *atomic.Value
}

func NewNodeSelectorWithConfig(config SelectorConfig) (Selector, error) {
	return NewNodeSelector(
		config.Strategy,
		config.NodeFilePath,
		config.MaxNodeNum,
		config.MaxTTL,
		config.FlushInterval,
	)
}

// NewNodeSelector create a new Selector for selecting a best peer list to storage file.
// strategy can be "fixed" or "priority", which means only using or priority using the specified node list respectively.
// nodeFilePath is the json file path of the node list you specify.
// maxNodeNum is used to specify the maximum available stable node.
// The units of parameters maxTTL and flushInterval are milliseconds and hours respectively.
func NewNodeSelector(strategy, nodeFilePath string, maxNodeNum int, maxTTL, flushInterval int64) (Selector, error) {
	selector := new(NodeSelector)
	if maxNodeNum <= 0 || maxNodeNum > MAX_ALLOWED_NODES {
		maxNodeNum = MAX_ALLOWED_NODES
	}
	maxTTL *= int64(time.Millisecond)
	if maxTTL <= 0 || maxTTL > int64(DEFAULT_MAX_TTL) {
		maxTTL = int64(DEFAULT_MAX_TTL)
	}
	flushInterval *= int64(time.Hour)
	if flushInterval <= 0 || flushInterval > int64(DEFAULT_FLUSH_TIME) {
		flushInterval = int64(DEFAULT_FLUSH_TIME)
	}

	var nodeList NodeList
	if _, err := os.Stat(nodeFilePath); err == nil {
		bytes, err := os.ReadFile(nodeFilePath)
		if err != nil {
			return nil, errors.Wrap(err, "create node selector error")
		}
		err = json.Unmarshal(bytes, &nodeList)
		if err != nil {
			return nil, errors.Wrap(err, "create node selector error")
		}
	} else {
		f, err := os.Create(nodeFilePath)
		if err == nil {
			f.WriteString(NODE_LIST_TEMP)
			f.Close()
		}
	}
	if len(nodeList.DisallowedPeers) > MAX_DISALLOWED_NODES {
		nodeList.DisallowedPeers =
			nodeList.DisallowedPeers[:MAX_DISALLOWED_NODES]
	}
	selector.blackList = bloom.NewWithEstimates(100000, 0.01)
	selector.listPeers = &sync.Map{}
	selector.lastOne = &atomic.Value{}
	selector.peerNum = &atomic.Int32{}
	selector.lastOne.Store(NodeInfo{})

	for _, peer := range nodeList.DisallowedPeers {
		selector.blackList.AddString(peer)
	}

	count := 0
	for key, addrStr := range nodeList.AllowedPeers {
		if count > MAX_ALLOWED_NODES {
			break
		}
		addr, err := multiaddr.NewMultiaddr(addrStr)
		var ttl time.Duration
		var addrs []multiaddr.Multiaddr
		if err == nil {
			ttl = GetConnectTTL([]multiaddr.Multiaddr{addr}, DEFAULT_TIMEOUT)
			addrs = []multiaddr.Multiaddr{addr}
		}
		bk, err := base58.Decode(key)
		if err != nil {
			continue
		}
		info := NodeInfo{
			AddrInfo: peer.AddrInfo{
				ID:    peer.ID(bk),
				Addrs: addrs,
			},
			FlushTime: time.Now(),
			Available: ttl > 0 && ttl <= time.Duration(maxTTL),
			TTL:       ttl,
		}
		selector.listPeers.Store(key, info)
		selector.peerNum.Add(1)
		count++
	}

	switch strategy {
	case PRIORITY_STRATEGY, FIXED_STRATEGY:
	default:
		strategy = DEFAULT_STRATEGY
	}
	selector.config = SelectorConfig{
		MaxNodeNum:    maxNodeNum,
		MaxTTL:        maxTTL,
		Strategy:      strategy,
		NodeFilePath:  nodeFilePath,
		FlushInterval: flushInterval,
	}
	selector.activePeers = &sync.Map{}
	return selector, nil
}

// GetPeer gets a node from the iterator and removes it from the iterator
func (c *NodeMap) GetPeer() (peer.AddrInfo, bool) {
	var addr peer.AddrInfo
	var ok bool
	for k, v := range c.nodes {
		addr = v
		ok = true
		delete(c.nodes, k)
		break
	}
	return addr, ok
}

// FlushlistedPeerNodes is used to update the status information of the node specified by the configuration file in the node selector
func (s *NodeSelector) FlushlistedPeerNodes(pingTimeout time.Duration, discoverer Discoverer) {
	s.listPeers.Range(func(key, value any) bool {
		k := key.(string)
		v := value.(NodeInfo)
		if (!v.Available && time.Since(v.FlushTime) < time.Hour) ||
			v.Available && time.Since(v.FlushTime) < time.Duration(s.config.FlushInterval) {
			return true
		}
		bk, err := base58.Decode(k)
		if err != nil {
			return true
		}
		addr, err := discoverer.FindPeer(context.Background(), peer.ID(bk))
		if err != nil {
			return true
		}
		v.AddrInfo = addr
		v.FlushTime = time.Now()
		v.TTL = GetConnectTTL(addr.Addrs, pingTimeout)
		if v.TTL > 0 && v.TTL < time.Duration(s.config.MaxTTL) {
			v.Available = true
		}
		s.listPeers.Swap(k, v)
		return true
	})
}

func (s *NodeSelector) FlushPeerNodes(pingTimeout time.Duration, peers ...peer.AddrInfo) {

	for _, peer := range peers {

		key := peer.ID.String()
		if s.blackList.TestString(key) {
			continue
		}
		swap := false
		info := NodeInfo{
			AddrInfo:  peer,
			FlushTime: time.Now(),
		}
		point := s.activePeers
		if value, ok := s.listPeers.Load(key); ok {
			v := value.(NodeInfo)
			if (!v.Available && time.Since(v.FlushTime) < time.Hour) ||
				(v.Available && time.Since(v.FlushTime) < time.Duration(s.config.FlushInterval)) {
				continue
			}
			info.NePoints = v.NePoints
			point = s.listPeers
		} else if value, ok := s.activePeers.Load(key); ok {
			v := value.(NodeInfo)
			if (!v.Available && time.Since(v.FlushTime) < time.Hour) ||
				(v.Available && time.Since(v.FlushTime) < time.Duration(s.config.FlushInterval)) {
				continue
			}
			info.NePoints = v.NePoints
		} else {
			if s.peerNum.Load() >= int32(s.config.MaxNodeNum) {
				swap = true
			}
			s.peerNum.Add(1)
		}
		info.TTL = GetConnectTTL(peer.Addrs, pingTimeout)
		if info.TTL > 0 && info.TTL < time.Duration(s.config.MaxTTL) {
			info.Available = true
		}
		point.Store(key, info)

		lastOne := s.lastOne.Load().(NodeInfo)
		if !swap {
			if lastOne.TTL < info.TTL {
				s.lastOne.Store(info)
			}
		} else if lastOne.TTL-info.TTL > PEER_NODES_GAP {
			s.activePeers.Delete(lastOne.AddrInfo.ID.String())
			s.peerNum.Add(-1)
			var max time.Duration
			s.activePeers.Range(func(key, value any) bool {
				v := value.(NodeInfo)
				if v.TTL > max {
					s.lastOne.Store(v)
					max = v.TTL
				}
				return true
			})
		}
	}
}

// GetPeersNumber gets the number of all valid nodes in the node selector
func (s *NodeSelector) GetPeersNumber() int {
	return int(s.peerNum.Load())
}

// NewPeersIteratorWithConditions is used to create a node iterator containing custom selection logic
func (s *NodeSelector) NewPeersIteratorWithConditions(minNum, maxNum int, conds ...func(key string, value NodeInfo) bool) (Iterator, error) {
	if minNum > s.config.MaxNodeNum || minNum > int(s.peerNum.Load()) {
		return nil, errors.Wrap(errors.New("not enough nodes"), "create peers iterator error")
	}
	if maxNum < minNum {
		maxNum = minNum
	}
	nodeCh := &NodeMap{
		nodes: make(map[string]peer.AddrInfo),
	}
	handle := func(key, value any) bool {
		k := key.(string)
		v := value.(NodeInfo)
		ok := true
		for _, condFunc := range conds {
			ok = ok && condFunc(k, v)
			if !ok {
				return true
			}
		}
		nodeCh.nodes[k] = v.AddrInfo
		return true
	}
	s.listPeers.Range(handle)

	if s.config.Strategy != FIXED_STRATEGY {
		s.activePeers.Range(handle)
	}
	if nodeCh.count < minNum {
		return nil, errors.Wrap(errors.New("not enough nodes"), "create peers iterator error")
	}
	return nodeCh, nil
}

// NewPeersIterator creates an iterator that selects nodes based on the strategy.
// These nodes may come from the list specified in the configuration file or other nodes discovered dynamically.
func (s *NodeSelector) NewPeersIterator(minNum int) (Iterator, error) {
	if minNum > s.config.MaxNodeNum || minNum > int(s.peerNum.Load()) {
		return nil, errors.Wrap(errors.New("not enough nodes"), "create peers iterator error")
	}
	maxNum := 3 * minNum //Triple node redundancy
	handle, nodeCh := s.newSelectFunc(maxNum)
	s.listPeers.Range(handle)

	if s.config.Strategy != FIXED_STRATEGY {
		s.activePeers.Range(handle)
	}
	if nodeCh.count < minNum {
		return nil, errors.Wrap(errors.New("not enough nodes"), "create peers iterator error")
	}
	return nodeCh, nil
}

// NewPeersIteratorWithListPeers creates an iterator that contains only the nodes set in the configuration file.
func (s *NodeSelector) NewPeersIteratorWithListPeers(minNum int) (Iterator, error) {
	if minNum > s.config.MaxNodeNum || minNum > int(s.peerNum.Load()) {
		return nil, errors.Wrap(errors.New("not enough nodes"), "create peers iterator error")
	}
	maxNum := 3 * minNum //Triple node redundancy
	handle, nodeCh := s.newSelectFunc(maxNum)
	s.listPeers.Range(handle)
	if nodeCh.count < minNum {
		return nil, errors.Wrap(errors.New("not enough nodes"), "create peers iterator error")
	}
	return nodeCh, nil
}

// NewPeersIteratorWithActivePeers creates an iterator that does not contain the nodes set in the configuration file.
// This also requires the selection strategy to support the selector to dynamically discover nodes.
func (s *NodeSelector) NewPeersIteratorWithActivePeers(minNum int) (Iterator, error) {
	if minNum > s.config.MaxNodeNum || minNum > int(s.peerNum.Load()) {
		return nil, errors.Wrap(errors.New("not enough nodes"), "create peers iterator error")
	}
	maxNum := 3 * minNum //Triple node redundancy
	handle, nodeCh := s.newSelectFunc(maxNum)
	if s.config.Strategy != FIXED_STRATEGY {
		s.activePeers.Range(handle)
	}
	if nodeCh.count < minNum {
		return nil, errors.Wrap(errors.New("not enough nodes"), "create peers iterator error")
	}
	return nodeCh, nil
}

func (s *NodeSelector) newSelectFunc(maxNum int) (func(key, value any) bool, *NodeMap) {

	nodeCh := &NodeMap{
		nodes: make(map[string]peer.AddrInfo),
	}
	aux := make([][]string, MAX_FAILED_CONN)
	for i := 0; i < MAX_FAILED_CONN; i++ {
		aux[i] = make([]string, 0)
	}
	handle := func(key, value any) bool {
		k := key.(string)
		v := value.(NodeInfo)
		if !v.Available || v.NePoints >= MAX_FAILED_CONN {
			return true
		}
		aux[v.NePoints] = append(aux[v.NePoints], k)
		if nodeCh.count < maxNum {
			nodeCh.nodes[k] = v.AddrInfo
			nodeCh.count++
			return true
		}
		for i := MAX_FAILED_CONN - 1; i >= 0; i-- {
			if v.NePoints >= i {
				break
			}
			if j := len(aux[i]) - 1; j >= 0 {
				nodeCh.nodes[k] = v.AddrInfo
				delete(nodeCh.nodes, aux[i][j])
				aux[i] = aux[i][:len(aux[i])-1]
				break
			}
		}
		return true
	}
	return handle, nodeCh
}

// ClearBlackList is used to clear the blacklist of node selectors
func (s *NodeSelector) ClearBlackList() {
	s.blackList.ClearAll()
	s.listPeers.Range(func(key, value any) bool {
		v := value.(NodeInfo)
		v.NePoints = 0
		s.listPeers.Store(key, v)
		return true
	})
}

// Feedback is used to receive user feedback on a specific node in the node selector
func (s *NodeSelector) Feedback(id string, isWrok bool) {
	if isWrok {
		s.reflashPeer(id)
	} else {
		s.removePeer(id)
	}
}

func (s *NodeSelector) reflashPeer(id string) {
	if s.blackList.TestString(id) {
		return
	}
	var (
		info  NodeInfo
		point *sync.Map
	)
	if v, ok := s.listPeers.Load(id); ok {
		info = v.(NodeInfo)
		point = s.listPeers
	} else if v, ok := s.activePeers.Load(id); ok {
		info = v.(NodeInfo)
		point = s.activePeers
	}
	if point != nil {
		info.NePoints = 0
		if time.Since(info.FlushTime) > time.Hour {
			info.TTL = GetConnectTTL(info.AddrInfo.Addrs, DEFAULT_TIMEOUT)
			info.Available = info.TTL > 0 && info.TTL <= time.Duration(s.config.MaxTTL)
			info.FlushTime = time.Now()
		}
		point.Store(id, info)
	}
}

func (s *NodeSelector) removePeer(id string) {
	if s.blackList.TestString(id) {
		return
	}
	if v, ok := s.listPeers.Load(id); ok {
		info := v.(NodeInfo)
		info.NePoints++
		if time.Since(info.FlushTime) > time.Hour {
			info.TTL = GetConnectTTL(info.AddrInfo.Addrs, DEFAULT_TIMEOUT)
			info.Available = info.TTL > 0 && info.TTL <= time.Duration(s.config.MaxTTL)
			info.FlushTime = time.Now()
		}
		s.listPeers.Store(id, info)
		return
	}
	if v, ok := s.activePeers.Load(id); ok {
		info := v.(NodeInfo)
		info.NePoints++
		if info.NePoints < MAX_FAILED_CONN {
			info.TTL = GetConnectTTL(info.AddrInfo.Addrs, DEFAULT_TIMEOUT)
			info.Available = info.TTL > 0 && info.TTL <= time.Duration(s.config.MaxTTL)
			info.FlushTime = time.Now()
			s.activePeers.Store(id, info)
		}
		s.activePeers.Delete(id)
		s.blackList.AddString(id)
		s.peerNum.Add(-1)
	}
}

func GetConnectTTL(addrs []multiaddr.Multiaddr, timeout time.Duration) time.Duration {
	var minTTL time.Duration = timeout
	for _, addr := range addrs {
		ip := strings.Split(addr.String(), "/")[2]
		// do not filter specified intranet ip
		if ok, err := utils.IsIntranetIpv4(ip); len(addrs) > 1 &&
			(err != nil || ok) {
			continue
		}
		ttl, err := utils.PingNode(ip, timeout)
		if err != nil {
			continue
		}
		if ttl < minTTL {
			minTTL = ttl
		}
	}
	if minTTL == timeout {
		minTTL = 0
	}
	return minTTL
}
