package peer

import (
	"github.com/honeycombio/refinery/config"
	"github.com/sirupsen/logrus"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"
)

type dnsPeers struct {
	c         config.Config
	peers     []string
	peerLock  sync.Mutex
	callbacks []func()
}

func newDnsPeers(c config.Config, done chan struct{}) (Peers, error) {
	peers := &dnsPeers{
		c: c,
	}
	err := peers.getFromDns()
	if err != nil {
		return nil, err
	}
	go peers.watchPeers(done)

	return peers, nil
}

func (p *dnsPeers) getFromDns() error {
	lookupAddr, err := p.c.GetDnsLookupAddr()
	if err != nil {
		return err
	}

	ips, err := net.LookupIP(lookupAddr)
	if err != nil {
		return err
	}

	var addrs []string

	for _, ip := range ips {
		port, err := p.c.GetDnsRemotePort()
		if err != nil {
			return err
		}
		addr := net.JoinHostPort(ip.String(), strconv.Itoa(port))
		addrs = append(addrs, addr)
	}

	p.peerLock.Lock()
	p.peers = addrs
	p.peerLock.Unlock()

	return nil
}

func (p *dnsPeers) GetPeers() ([]string, error) {
	p.peerLock.Lock()
	defer p.peerLock.Unlock()
	retList := make([]string, len(p.peers))
	copy(retList, p.peers)
	return retList, nil
}

func (p *dnsPeers) watchPeers(done chan struct{}) {
	oldPeerList := p.peers
	sort.Strings(oldPeerList)
	tk := time.NewTicker(refreshCacheInterval)

	for {
		select {
		case <-tk.C:
			currentPeers, err := p.GetPeers()
			if err != nil {
				logrus.WithError(err).
					WithFields(logrus.Fields{
						"timeout":  p.c.GetPeerTimeout().String(),
						"oldPeers": oldPeerList,
					}).
					Error("get members failed during watch")
				continue
			}

			sort.Strings(currentPeers)
			if !equal(oldPeerList, currentPeers) {
				// update peer list and trigger callbacks saying the peer list has changed
				p.peerLock.Lock()
				p.peers = currentPeers
				oldPeerList = currentPeers
				p.peerLock.Unlock()
				for _, callback := range p.callbacks {
					// don't block on any of the callbacks.
					go callback()
				}
			}
		case <-done:
			p.peerLock.Lock()
			p.peers = []string{}
			p.peerLock.Unlock()
			return
		}
	}
}

func (p *dnsPeers) RegisterUpdatedPeersCallback(callback func()) {
	p.callbacks = append(p.callbacks, callback)
}
