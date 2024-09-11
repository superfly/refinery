package peer

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/sirupsen/logrus"
)

var (
	internalAddr string = fmt.Sprintf("%s.%s.internal", os.Getenv("FLY_REGION"), os.Getenv("FLY_APP_NAME"))
	peerPort     int    = 8193
)

type DnsPeers struct {
	c         config.Config
	peers     []string
	peerLock  sync.Mutex
	callbacks []func()
}

func NewDnsPeers(c config.Config, done chan struct{}) (Peers, error) {
	peers := &DnsPeers{
		c: c,
	}
	peerList, err := peers.getFromDns()
	if err != nil {
		return nil, err
	}

	peers.peerLock.Lock()
	peers.peers = peerList
	peers.peerLock.Unlock()

	go peers.watchPeers(done)

	return peers, nil
}

func (p *DnsPeers) getFromDns() ([]string, error) {
	ips, err := net.LookupIP(internalAddr)
	if err != nil {
		return nil, err
	}

	var addrs []string
	for _, ip := range ips {
		addr := url.URL{
			Scheme: "http",
			Host:   net.JoinHostPort(ip.String(), strconv.Itoa(peerPort)),
		}
		addrs = append(addrs, addr.String())
	}

	return addrs, nil
}

func (p *DnsPeers) GetPeers() ([]string, error) {
	p.peerLock.Lock()
	defer p.peerLock.Unlock()
	retList := make([]string, len(p.peers))
	copy(retList, p.peers)
	return retList, nil
}

func (p *DnsPeers) GetInstanceID() (string, error) {
	p.peerLock.Lock()
	defer p.peerLock.Unlock()
	machineID, _ := os.Hostname()

	appName := os.Getenv("FLY_APP_NAME")
	sixpnAddr := fmt.Sprintf("%s.vm.%s.internal", machineID, appName)

	ips, err := net.LookupIP(sixpnAddr)
	if err != nil {
		return "", err
	}

	if len(ips) < 0 {
		return "", fmt.Errorf("dns result empty")
	}

	addr := url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(ips[0].String(), strconv.Itoa(peerPort)),
	}

	return addr.String(), nil
}

func (p *DnsPeers) Start() (err error) {
	return nil
}

func (p *DnsPeers) Ready() error {
	return nil
}

func (p *DnsPeers) watchPeers(done chan struct{}) {
	oldPeerList := p.peers
	sort.Strings(oldPeerList)
	tk := time.NewTicker(refreshCacheInterval)

	for {
		select {
		case <-tk.C:
			currentPeers, err := p.getFromDns()
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

func (p *DnsPeers) RegisterUpdatedPeersCallback(callback func()) {
	p.callbacks = append(p.callbacks, callback)
}

// equal tells whether a and b contain the same elements.
// A nil argument is equivalent to an empty slice.
// lifted from https://yourbasic.org/golang/compare-slices/
func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
