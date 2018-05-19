package RPC

import (
	"errors"
	"log"
	"net/rpc"
	"os"
)

type ClientEnd struct {
	address string      // Remote address
	client  *rpc.Client // RPC client
	ni      *Network    // Pointer to network information
	self    int         // Index of self
	remote  int         // Index of remote
}

const (
	RPClog = false
)

var (
	logFile, _ = os.OpenFile("RPCLog.log", os.O_RDWR|os.O_CREATE, 0777)
	logger     = log.New(logFile, "", log.LstdFlags)
)

func (c *ClientEnd) Call(svcMthd string, args interface{}, reply interface{}) error {
	// Remote server is offline
	if !c.ni.connected[c.remote] {
		return errors.New("Cann't connect to remote server")
	}

	err := c.client.Call(svcMthd, args, reply)

	if RPClog {
		logger.Printf("[RPC] Method:%s Args:%+v Reply:%+v Status:%v\n", svcMthd, args, reply, err)
	}

	return err
}

func (c *ClientEnd) Connect() {
	client, err := rpc.DialHTTP("tcp", c.address)
	if err != nil {
		panic("RPC:[rpc.DialHTTP]:" + err.Error())
	}
	c.client = client
}

// NewClientEnd is use to create a new instance of ClientEnd
func NewClientEnd(svradd string, ni *Network, self int, rem int) *ClientEnd {
	c := ClientEnd{
		address: svradd,
		ni:      ni,
		self:    self,
		remote:  rem,
	}
	return &c
}

// Network is a struct use to hold all network information, every rpc client have a pointer
// to this function
type Network struct {
	connected []bool
	drop      int // Drop rate is drop/100
	latency   int // Latency in millsecond
	n         int
}

// Enable is use to set if a server is enabled
func (ni *Network) Enable(index int, enable bool) {
	ni.connected[index] = enable
}

// SetLatency is use to set max latency in this network
func (ni *Network) SetLatency(lat int) {
	ni.latency = lat
}

// SetDrop is use to set drop rate between every pair of cs
func (ni *Network) SetDrop(drop int) {
	ni.drop = drop
}

// NewNetwork is use to create a network struct, which hold all connect,drop,latency information
func NewNetwork(n int) *Network {
	ni := Network{
		connected: make([]bool, n),
		drop:      0,
		latency:   0,
		n:         n,
	}
	for i := 0; i < n; i++ {
		ni.connected[i] = true
	}
	return &ni
}
