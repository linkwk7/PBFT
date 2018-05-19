package pbft

import (
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "net/http/pprof"

	"../RPC"
	"../Util"
)

type config struct {
	cn      int
	sn      int
	n       int
	f       int
	servers []*PBFTServer
	clients []*PBFTClient
	svradd  []string
	ends    [][]*RPC.ClientEnd
	rpcsvr  []*rpc.Server
	ni      *RPC.Network
}

func testBasic(t *testing.T, cnum int, reqnum int, pause time.Duration) {
	c := config{}
	c.init(cnum)

	wg := sync.WaitGroup{}

	for i := 0; i < c.cn; i++ {
		wg.Add(1)
		go func(cid int) {
			for j := 0; j < reqnum; j++ {
				c.clients[cid].Start(strconv.Itoa(j) + "-" + strconv.Itoa(cid))
				time.Sleep(pause)
			}
			wg.Done()
		}(i)
		wg.Add(1)
		go func(cid int) {
			for j := 0; j < reqnum; j++ {
				res := <-c.clients[cid].apply
				log.Printf("%s:[Apply]:%+v\n", c.clients[cid], res)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	time.Sleep(5 * time.Second)
}

func TestOne(t *testing.T) {
	testBasic(t, 1, 1, 2*time.Millisecond)
}

func TestMultiple(t *testing.T) {
	testBasic(t, 1, 100000, 2*time.Millisecond)
}

func TestMultiClientLowLoad(t *testing.T) {
	testBasic(t, 2, 1000, 2*time.Millisecond)
}

func TestMultiClientHighLoad(t *testing.T) {
	testBasic(t, 2, 50000, 2*time.Millisecond)
}

func TestDisconnectFollower(t *testing.T) {
	testDisconnect(t, 1, 10000, 2*time.Millisecond)
}

func TestDisconnectLeader(t *testing.T) {
	c := config{}
	c.init(1)

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		for j := 0; j < 201; j++ {
			res := <-c.clients[0].apply
			log.Printf("%s:[Apply]:%+v\n", c.clients[0], res)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for j := 0; j < 200; j++ {
			c.clients[0].Start(strconv.Itoa(j) + "-" + strconv.Itoa(0))
			time.Sleep(2 * time.Millisecond)
		}

		c.ni.Enable(0+c.cn, false)
		Util.Dprintf("Disconnect replica 0")

		for j := 200; j < 201; j++ {
			c.clients[0].Start(strconv.Itoa(j) + "-" + strconv.Itoa(0))
			time.Sleep(2 * time.Millisecond)
		}
		wg.Done()
	}()

	wg.Wait()
	time.Sleep(5 * time.Second)
}

func testDisconnect(t *testing.T, rid int, reqnum int, pause time.Duration) {
	c := config{}
	c.init(1)

	c.ni.Enable(rid+c.cn, false)

	wg := sync.WaitGroup{}

	for i := 0; i < c.cn; i++ {
		wg.Add(1)
		go func(cid int) {
			for j := 0; j < reqnum; j++ {
				c.clients[cid].Start(strconv.Itoa(j) + "-" + strconv.Itoa(cid))
				time.Sleep(pause)
			}
			wg.Done()
		}(i)
		wg.Add(1)
		go func(cid int) {
			for j := 0; j < reqnum; j++ {
				res := <-c.clients[cid].apply
				log.Printf("%s:[Apply]:%+v\n", c.clients[cid], res)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	time.Sleep(5 * time.Second)
}

func (c *config) init(cn int) {
	c.cn = cn
	c.sn = 4
	c.n = c.cn + c.sn
	c.f = 1
	c.servers = make([]*PBFTServer, c.sn)
	c.clients = make([]*PBFTClient, c.cn)
	c.svradd = make([]string, c.n)
	c.ends = make([][]*RPC.ClientEnd, c.n)
	c.rpcsvr = make([]*rpc.Server, c.n)
	c.ni = RPC.NewNetwork(c.n)
	for i, base := 0, 10000; i < c.n; i, base = i+1, base+1 {
		c.svradd[i] = "127.0.0.1:" + strconv.Itoa(base+(rand.Int()%200))
	}
	for i := 0; i < c.n; i++ {
		c.start1(i, i < c.cn)
	}
	for i := 0; i < c.n; i++ {
		for j := 0; j < c.n; j++ {
			c.ends[i][j].Connect()
		}
	}
}

// Start rpc server and create n client connect to all servers
func (c *config) start1(index int, ic bool) {
	ends := make([]*RPC.ClientEnd, c.n)
	svr := rpc.NewServer()
	for i := 0; i < c.n; i++ {
		ends[i] = RPC.NewClientEnd(c.svradd[i], c.ni, index, i)
	}
	if ic {
		c.clients[index] = NewPBFTClient(index, c.f, ends[c.cn:])
		svr.Register(c.clients[index])
	} else {
		c.servers[index-c.cn] = NewPBFTServer(index-c.cn, c.f, ends[:c.cn], ends[c.cn:])
		svr.Register(c.servers[index-c.cn])
	}

	// Solution from https://github.com/golang/go/issues/13395 to avoid panic: http: multiple registrations for /_goRPC_
	oldMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux
	svr.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux

	_, port, e := net.SplitHostPort(c.svradd[index])
	if e != nil {
		panic("Net:[net.SplitHostPort]" + e.Error())
	}
	l, e := net.Listen("tcp", ":"+port)
	if e != nil {
		panic("RPC:[net.Listen]" + e.Error())
	}
	go http.Serve(l, mux)
	c.ends[index] = ends
}
