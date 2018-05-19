package RPC

import (
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"testing"
)

type TS struct {
	val int
}

func (s *TS) Set(args int, reply *int) error {
	s.val = args
	return nil
}

func (s *TS) Get(args int, reply *int) error {
	*reply = s.val
	return nil
}

// Solution from https://github.com/golang/go/issues/13395 to avoid panic: http: multiple registrations for /_goRPC_
func newService(add string, v int) {
	ts := &TS{v}
	svr := rpc.NewServer()
	svr.Register(ts)

	oldMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux
	svr.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux

	l, e := net.Listen("tcp", add)
	if e != nil {
		panic("RPC:[net.Listen]" + e.Error())
	}
	go http.Serve(l, mux)
}

func comm(add string, v int, ni *Network, self int, rem int, t *testing.T) {
	end := NewClientEnd(add, ni, self, rem)
	end.Connect()

	val := 0
	tr := 0
	err := end.Call("TS.Get", 0, &val)
	if err != nil {
		t.Error(err.Error())
	}
	if val != v {
		t.Error("[TS.Get]Get " + strconv.Itoa(val) + " should be " + strconv.Itoa(v))
	}
	val++
	err = end.Call("TS.Set", val, &tr)
	if err != nil {
		t.Error(err.Error())
	}
	val = 0
	err = end.Call("TS.Get", 0, &val)
	if err != nil {
		t.Error(err.Error())
	}
	if val != v+1 {
		t.Error("[TS.Get]Get " + strconv.Itoa(val) + " should be " + strconv.Itoa(v+1))
	}
}

func TestBasic(t *testing.T) {
	ni := NewNetwork(4)

	newService(":5678", 1)
	newService(":5679", 2)

	comm("127.0.0.1:5678", 1, ni, 0, 2, t)
	comm("127.0.0.1:5679", 2, ni, 1, 3, t)
}

func TestNetwork(t *testing.T) {
	ni := NewNetwork(3)
	ends := make([]*ClientEnd, 2)

	newService(":5678", 1)
	ends[0] = NewClientEnd("127.0.0.1:5678", ni, 0, 2)
	ends[1] = NewClientEnd("127.0.0.1:5678", ni, 1, 2)
	ends[0].Connect()
	ends[1].Connect()
	val0, val1 := 0, 0
	ends[0].Call("TS.Get", 0, &val0)
	ends[1].Call("TS.Get", 0, &val1)
	if val0 != 1 || val1 != 1 {
		t.Error("Get error")
	}

	ni.Enable(2, false)
	ends[0].Call("TS.Set", 3, &val0)
	ends[0].Call("TS.Get", 0, &val0)
	if val0 == 3 {
		t.Error("Set error")
	}
	ends[1].Call("TS.Set", 4, &val1)
	ends[1].Call("TS.Get", 0, &val0)
	if val0 == 4 {
		t.Error("Set error")
	}

	ni.Enable(2, true)
	ends[0].Call("TS.Set", 3, &val0)
	ends[0].Call("TS.Get", 0, &val0)
	if val0 != 3 {
		t.Error("Get error")
	}
	ends[1].Call("TS.Set", 4, &val1)
	ends[0].Call("TS.Get", 0, &val0)
	if val0 != 4 {
		t.Error("Get error")
	}

	ni.Enable(0, false)
	ends[0].Call("TS.Set", 5, &val0)
	ends[0].Call("TS.Get", 0, &val0)
	if val0 == 5 {
		t.Error("Set error")
	}
	ends[1].Call("TS.Set", 6, &val1)
	ends[1].Call("TS.Get", 0, &val0)
	if val0 != 6 {
		t.Error("Set error")
	}
}
