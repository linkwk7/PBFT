// TODO : Add apply queue
// TODO : client didn't send
package pbft

import (
	"fmt"
	"sync"
	"time"

	"../RPC"
	"../Util"
)

const (
	retransmitTimeout = 200 * time.Millisecond
	outstanding       = 200
)

type RequestArgs struct {
	Op  interface{}
	Seq int
	Id  int
	// Signature
}

type RequestReply struct {
	Err string
}

type cEntry struct {
	lock  sync.Mutex
	apply bool
	req   *RequestArgs
	res   []*ResponseArgs
	t     *time.Timer
	ft    bool
}

type PBFTClient struct {
	lock         sync.Mutex // Lock for internal data
	id           int
	seq          int // Sequence number of next request
	seqMutex     sync.Mutex
	view         int
	replicas     []*RPC.ClientEnd
	f            int // Max number of fail node
	log          map[int]*cEntry
	reqQueue     *Util.PriorityQueue // Use to send request to daemon routine
	backupQueue  *Util.PriorityQueue // Use to receive request but not in reqQueue
	response     *Util.PriorityQueue // Use to record which sequence number have already receive enough response
	reqch        chan bool
	apply        chan interface{}
	lastResponse int
	count        int
}

// Start is the only export function of PBFTClient, use to start a consensus
// Retry is necessary, cause process is very slow, it will take some time to accomplish consensus,
// and the sequence number space will quickly run out
func (c *PBFTClient) Start(cmd interface{}) error {
	c.lock.Lock()

	Util.Dprintf("%s[Start]:Cmd:%s\n", c, cmd)

	args := RequestArgs{
		Op:  cmd,
		Seq: c.seq,
		Id:  c.id,
	}
	c.seq++
	ent := c.getCEntry(args.Seq)
	ent.req = &args

	if args.Seq <= c.lastResponse+outstanding {
		Util.Dprintf("%s[Add2Req]%+v", c, args)
		c.reqQueue.Insert(Util.PQElem{Pri: args.Seq, C: ent})
		go func() {
			// Notify daemon to send request
			c.reqch <- true
		}()
	} else {
		Util.Dprintf("%s[Add2Backup]%+v", c, args)
		c.backupQueue.Insert(Util.PQElem{Pri: args.Seq, C: ent})
	}

	c.lock.Unlock()

	return nil
}

// ResponseArgs is the argument for RPC handler PBFTClient.Response
type ResponseArgs struct {
	View int
	Seq  int
	Cid  int // Client id
	Rid  int // Replica id
	Res  interface{}
	// Signature
}

// ResponseReply is the argument for RPC handler PBFTClient.Response
type ResponseReply struct {
}

// Response is the handler of RPC PBFTClient.Response
// ent.lock -> c.lock
func (c *PBFTClient) Response(args ResponseArgs, reply *ResponseReply) error {
	c.lock.Lock()
	ent := c.getCEntry(args.Seq)
	if args.View > c.view {
		c.view = args.View
	}
	c.lock.Unlock()

	Util.Dprintf("%s[R/Response]:Args:%+v", c, args)

	ent.lock.Lock()

	if ent.t != nil {
		ent.t.Stop()
	}
	if !ent.apply {
		ent.res = append(ent.res, &args)
		if len(ent.res) > c.f {
			// Map result to replicas list who send that result
			count := make(map[interface{}][]int)
			for i, sz := 0, len(ent.res); i < sz; i++ {
				v, ok := count[ent.res[i].Res]
				if !ok {
					count[ent.res[i].Res] = []int{ent.res[i].Rid}
				} else {
					count[ent.res[i].Res] = append(v, ent.res[i].Rid)
				}

				// TODO : Detect and resend
				if len(count[ent.res[i].Res]) > c.f && Util.DifferentElemInSlice(count[ent.res[i].Res]) > c.f {
					ent.apply = true

					c.lock.Lock()
					c.updateQueue(args.Seq)
					c.lock.Unlock()

					c.apply <- ent.res[i].Res

					break
				}
			}
		}
	}

	ent.lock.Unlock()

	return nil
}

// Update response queue and move some request from backupQueue to reqQueue
func (c *PBFTClient) updateQueue(seq int) {
	c.response.Insert(Util.PQElem{Pri: seq, C: nil})

	for cseq := c.lastResponse + 1; true; cseq++ {
		elem, err := c.response.GetMin()
		if err != nil {
			// Response queue is empty
			break
		}
		if elem.Pri < cseq {
			panic("Priority queue extract something bigger than expect")
		} else if elem.Pri > cseq {
			break
		} else {
			c.lastResponse++
			c.response.ExtractMin()
		}
	}

	// Move request from backup queue to request queue
	for {
		mReq, err := c.backupQueue.GetMin()
		if err != nil {
			// Nothing in backup queue
			break
		}
		if mReq.Pri <= c.lastResponse+outstanding {
			Util.Dprintf("%s[MoveBackup2Req]@%v", c, mReq.Pri)
			c.backupQueue.ExtractMin()
			c.reqQueue.Insert(mReq)
			go func() {
				c.reqch <- true
			}()
		} else {
			break
		}
	}
}

func (c *PBFTClient) daemon() {
	for {
		<-c.reqch

		// Extract element from request queue
		c.lock.Lock()
		elem, err := c.reqQueue.ExtractMin()
		if err != nil {
			panic("Receive from channel but nothing in queue")
		}
		c.lock.Unlock()

		ent := elem.C.(*cEntry)
		ent.lock.Lock()

		if ent.t != nil {
			ent.t.Stop()
		}
		if !ent.apply {
			ent.t = time.AfterFunc(retransmitTimeout, func() {
				c.lock.Lock()

				Util.Dprintf("%s[Retransmit]%+v", c, ent.req)
				c.reqQueue.Insert(Util.PQElem{Pri: ent.req.Seq, C: ent})

				c.lock.Unlock()

				c.reqch <- true
			})

			if ent.ft {
				Util.Dprintf("%s[S/Request]:Args:%+v", c, *(ent.req))

				ent.ft = false
				leader := c.view % len(c.replicas)
				go func(rid int) {
					reply := RequestReply{}
					c.replicas[rid].Call("PBFTServer.Request", *(ent.req), &reply)
				}(leader)
			} else {
				Util.Dprintf("%s[B/Request]:Args:%+v", c, *(ent.req))

				for i, sz := 0, len(c.replicas); i < sz; i++ {
					go func(rid int) {
						reply := RequestReply{}
						c.replicas[rid].Call("PBFTServer.Request", *(ent.req), &reply)
					}(i)
				}
			}
		}

		ent.lock.Unlock()
	}
}

func (c *PBFTClient) String() string {
	s := fmt.Sprintf("{C:ID:%d,Seq:%d,View:%d}", c.id, c.seq, c.view)
	return s
}

func (c *PBFTClient) getCEntry(seq int) *cEntry {
	_, ok := c.log[seq]
	if !ok {
		c.log[seq] = &cEntry{
			apply: false,
			req:   nil,
			res:   make([]*ResponseArgs, 0),
			t:     nil,
			ft:    true,
		}
	}
	return c.log[seq]
}

// NewPBFTClient use given information to create a new pbft client
func NewPBFTClient(cid int, f int, r []*RPC.ClientEnd) *PBFTClient {
	c := PBFTClient{
		id:           cid,
		seq:          0,
		view:         0,
		replicas:     r,
		f:            f,
		log:          make(map[int]*cEntry),
		reqQueue:     Util.NewPriorityQueue(),
		backupQueue:  Util.NewPriorityQueue(),
		response:     Util.NewPriorityQueue(),
		reqch:        make(chan bool, outstanding),
		apply:        make(chan interface{}),
		lastResponse: -1,
	}

	go c.daemon()

	return &c
}
