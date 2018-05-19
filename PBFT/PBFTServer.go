package pbft

import (
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"../RPC"
	"../Util"
)

const (
	changeViewTimeout = 500 * time.Millisecond
	checkpointDiv     = 200
)

type PBFTServer struct {
	lock       sync.Mutex // Lock for all internal data
	id         int
	tSeq       int             // Total sequence number of next request
	seq        []int           // Sequence number of next client request
	seqmap     map[entryID]int // Use to map {Cid,CSeq} to global sequence number for all prepared message
	view       int
	apply      int // Sequence number of last executed request
	log        map[entryID]*entry
	cps        map[int]*CheckPoint
	h          int
	H          int
	f          int
	monitor    bool
	change     *time.Timer
	changing   bool // Indicate if this node is changing view
	clients    []*RPC.ClientEnd
	replicas   []*RPC.ClientEnd
	state      interface{} // Deterministic state machine's state
	applyQueue *Util.PriorityQueue
	vcs        map[int][]*ViewChangeArgs
	lastcp     int
	reqQueue   []*Util.PriorityQueue
}

type entryID struct {
	v int
	n int
}

type entry struct {
	lock       sync.Mutex
	pp         *PrePrepareArgs
	p          []*PrepareArgs
	sendCommit bool
	c          []*CommitArgs
	sendReply  bool
	r          *ResponseArgs
}

// pendingTask is use to represent the pending task
type pendingTask struct {
	seq  int
	op   interface{}
	done chan interface{}
	dgt  string
}

// Request is use to process incoming clients's request, if this peer is leader then process this request,
// otherwise it will relay the request to leader and start view change timer
// Locks : s.lock
func (s *PBFTServer) Request(args RequestArgs, reply *RequestReply) error {
	// TODO : verify message signature

	s.lock.Lock()
	leader := s.view % len(s.replicas)
	if !s.changing {
		if s.id == leader {
			s.handleRequestLeader(&args, reply)
		} else {
			s.handleRequestFollower(&args, reply)
		}
	}
	s.lock.Unlock()

	return nil
}

// Locks : should acquire s.lock before call this function
func (s *PBFTServer) handleRequestLeader(args *RequestArgs, reply *RequestReply) error {
	if args.Seq >= s.seq[args.Id] {
		Util.Dprintf("%s[Record/NewRequest]:Args:%+v,QueueLen:%v,Last:%v\n", s, args, s.reqQueue[args.Id].Length(), s.seq[args.Id])

		// Add current request to request queue
		s.reqQueue[args.Id].Insert(Util.PQElem{Pri: args.Seq, C: args})
		// Extract continuous requests from message queue of client and update sequence
		seq, _, reqlst := s.matchReq(args.Id)

		for i, sz := 0, len(reqlst); i < sz; i++ {
			ppArgs := PrePrepareArgs{
				View:    s.view,
				Seq:     seq + i,
				Digest:  Util.Digest(*(reqlst[i])),
				Message: *(reqlst[i]),
			}

			Util.Dprintf("%s[B/PrePrepare]:Args:%+v\n", s, ppArgs)

			s.broadcast(false, true, "PBFTServer.PrePrepare", ppArgs, &PrePrepareReply{})
		}
	} else {
		// Old request, if it already finish execute, try to find result and reply again
		// if it haven't done yet, just start again by broadcast
		gseq, ok := s.seqmap[entryID{args.Id, args.Seq}]
		// This message is prepared
		if ok {
			if gseq <= s.lastcp {
				// The log already been removed
				Util.Dprintf("%s[Request/Fail] log already been removed @ %v, CP:%v", s, gseq, s.lastcp)
				reply.Err = "OldRequest"
			} else {
				// Base on assert : leader will have all preprepare message for request after last checkpoint
				ent := s.getEntry(entryID{s.view, gseq})
				if ent.pp == nil {
					st := fmt.Sprintf("%s old request didn't have preprepare @ %v", s, args.Seq)
					panic(st)
					return nil
				}
				if ent.pp.Digest == "" {
					// Execute fail
					Util.Dprintf("%s[Request/Fail]:Args:%+v", s, args)
					reply.Err = "LostRequest"
				} else {
					if !ent.sendReply {
						Util.Dprintf("%s[Re/B/PrePrepare]:Args:%+v", s, *(ent.pp))
						s.broadcast(false, true, "PBFTServer.PrePrepare", *(ent.pp), &PrePrepareReply{})
					} else {
						s.reply(ent)
					}
				}
			}
		} else {
			// The request of client must lost when view change or some preprepare message is lost
			// If client receive more than f message about this, it will start another new message
			reply.Err = "LostRequest"
		}
	}

	return nil
}

// Locks : should acquire s.lock before call this funtion
func (s *PBFTServer) handleRequestFollower(args *RequestArgs, reply *RequestReply) error {
	if !s.monitor {
		s.monitor = true
		s.change = time.AfterFunc(changeViewTimeout, s.startViewChange)

		s.replicas[s.view%len(s.replicas)].Call("PBFTServer.Request", *args, reply)
	}

	return nil
}

// Locks : should acquire s.lock before call this function
func (s *PBFTServer) matchReq(cid int) (int, int, []*RequestArgs) {
	reqlst := make([]*RequestArgs, 0)

	for {
		if s.tSeq > s.H {
			break
		}

		elem, err := s.reqQueue[cid].GetMin()
		if err != nil {
			Util.Dprintf("%s[Queue is empty]", s)
			break
		}
		if elem.Pri < s.seq[cid] {
			panic("[Receive old request]" + s.String())
		} else if elem.Pri == s.seq[cid] {
			Util.Dprintf("Add2SendingQueue%v", elem.C.(*RequestArgs))
			reqlst = append(reqlst, elem.C.(*RequestArgs))
			s.reqQueue[cid].ExtractMin()
			s.seq[cid]++
			s.tSeq++
		} else {
			Util.Dprintf("%s[>]%v > %v", s, elem.Pri, s.seq[cid])
			break
		}
	}
	return s.tSeq - len(reqlst), s.seq[cid] - len(reqlst), reqlst
}

// PrePrepareArgs is the argument for RPC handler PBFTServer.PrePrepare
type PrePrepareArgs struct {
	View   int
	Seq    int
	Digest string
	// Signature
	Message RequestArgs
}

// PrePrepareReply is the reply for RPC handler PBFTServer.PrePrepare
type PrePrepareReply struct {
}

// PrePrepare is the handler of RPC PBFTServer.PrePrepare
// Locks : s.lock; ent.lock;
func (s *PBFTServer) PrePrepare(args PrePrepareArgs, reply *PrePrepareReply) error {
	// Verify message signature and digest

	s.lock.Lock()

	s.stopTimer()

	if !s.changing && s.view == args.View && s.h <= args.Seq && args.Seq < s.H {
		Util.Dprintf("%s[R/PrePrepare]:Args:%+v", s, args)

		ent := s.getEntry(entryID{args.View, args.Seq})
		s.lock.Unlock()

		ent.lock.Lock()
		if ent.pp == nil || (ent.pp.Digest == args.Digest && ent.pp.Seq == args.Seq) {
			pArgs := PrepareArgs{
				View:   args.View,
				Seq:    args.Seq,
				Digest: args.Digest,
				Rid:    s.id,
			}
			ent.pp = &args

			Util.Dprintf("%s[B/Prepare]:Args:%+v", s, pArgs)

			s.broadcast(false, true, "PBFTServer.Prepare", pArgs, &PrepareReply{})
		}
		ent.lock.Unlock()
	} else {
		s.lock.Unlock()
	}

	return nil
}

// PrepareArgs is the argument for RPC handler PBFTServer.Prepare
type PrepareArgs struct {
	View   int
	Seq    int
	Digest string
	Rid    int
	// Signature
}

// PrepareReply is the reply for RPC handler PBFTServer.Prepare
type PrepareReply struct {
}

// Prepare is the handler of RPC PBFTServer.Prepare
// Locks : s.lock; ent.lock -> s.lock;
func (s *PBFTServer) Prepare(args PrepareArgs, reply *PrepareReply) error {

	s.lock.Lock()

	s.stopTimer()

	if !s.changing && s.view == args.View && s.h <= args.Seq && args.Seq < s.H {
		ent := s.getEntry(entryID{args.View, args.Seq})
		s.lock.Unlock()

		ent.lock.Lock()

		Util.Dprintf("%s[R/Prepare]:Args:%+v", s, args)
		ent.p = append(ent.p, &args)
		if ent.pp != nil && (ent.sendCommit || s.prepared(ent)) {
			s.lock.Lock()
			// Update sequence map and follower's sequence
			cid, cseq := ent.pp.Message.Id, ent.pp.Message.Seq
			s.seqmap[entryID{cid, cseq}] = args.Seq
			if s.view%len(s.replicas) != s.id {
				if cseq > s.seq[cid] {
					s.seq[cid] = cseq
				}
			}
			s.lock.Unlock()

			cArgs := CommitArgs{
				View:   args.View,
				Seq:    ent.pp.Seq,
				Digest: ent.pp.Digest,
				Rid:    s.id,
			}
			ent.sendCommit = true

			Util.Dprintf("%s[B/Commit]:Args:%+v", s, cArgs)

			s.broadcast(false, true, "PBFTServer.Commit", cArgs, &CommitReply{})
		}
		ent.lock.Unlock()
	} else {
		s.lock.Unlock()
	}
	return nil
}

// Lock ent.lock before call this function
// Locks : acquire s.lock before call this function
func (s *PBFTServer) prepared(ent *entry) bool {
	if len(ent.p) > 2*s.f {
		// Key is the id of sender replica
		validSet := make(map[int]bool)
		for i, sz := 0, len(ent.p); i < sz; i++ {
			if ent.p[i].View == ent.pp.View && ent.p[i].Seq == ent.pp.Seq && ent.p[i].Digest == ent.pp.Digest {
				validSet[ent.p[i].Rid] = true
			}
		}
		return len(validSet) > 2*s.f
	}
	return false
}

// CommitArgs is the argument for RPC handler PBFTServer.Commit
type CommitArgs struct {
	View   int
	Seq    int
	Digest string
	Rid    int
}

// CommitReply is the reply for RPC handler PBFTServer.Commit
type CommitReply struct {
}

// Commit is the handler of RPC PBFTServer.Commit
// Locks : ent.lock -> s.lock
func (s *PBFTServer) Commit(args CommitArgs, reply *CommitReply) error {
	// Verify signature

	s.lock.Lock()

	s.stopTimer()

	if !s.changing && s.view == args.View && s.h <= args.Seq && args.Seq < s.H {
		ent := s.getEntry(entryID{args.View, args.Seq})
		s.lock.Unlock()

		ent.lock.Lock()
		ent.c = append(ent.c, &args)
		Util.Dprintf("%s[R/Commit]:Args:%+v", s, args)
		if !ent.sendReply && ent.sendCommit && s.committed(ent) {
			Util.Dprintf("%s start execute %v @ %v", s, ent.pp.Message.Op, args.Seq)
			// Execute will make sure there only one execution of one request
			res, _ := s.execute(args.Seq, ent.pp.Message.Op, args.Digest)
			if ent.r == nil {
				rArgs := ResponseArgs{
					View: args.View,
					Seq:  ent.pp.Message.Seq,
					Cid:  ent.pp.Message.Id,
					Rid:  s.id,
					Res:  res,
				}
				ent.r = &rArgs
			}
			ent.sendReply = true
		}
		s.reply(ent)
		ent.lock.Unlock()
	} else {
		s.lock.Unlock()
	}
	return nil
}

// Locks : acquire s.lock before call this function
func (s *PBFTServer) committed(ent *entry) bool {
	if len(ent.c) > 2*s.f {
		// Key is replica id
		validSet := make(map[int]bool)
		for i, sz := 0, len(ent.c); i < sz; i++ {
			if ent.c[i].View == ent.pp.View && ent.c[i].Seq == ent.pp.Seq && ent.c[i].Digest == ent.pp.Digest {
				validSet[ent.c[i].Rid] = true
			}
		}
		return len(validSet) > 2*s.f
	}
	return false
}

// Locks : acquire ent.lock before call this function
func (s *PBFTServer) reply(ent *entry) {
	if ent.r != nil {
		go func() {
			Util.Dprintf("%s[S/Reply]:Args:%+v", s, ent.r)

			reply := ResponseReply{}

			err := s.clients[ent.pp.Message.Id].Call("PBFTClient.Response", *ent.r, &reply)

			if err != nil {
			}
		}()
	} else {
		Util.Dprintf("%s[Trying reply] Fail", s)
	}
}

// Locks : s.lock
func (s *PBFTServer) execute(seq int, op interface{}, digest string) (interface{}, bool) {
	s.lock.Lock()

	if seq <= s.apply {
		// This is an old request, try to find it's result, if can't, return false
		s.lock.Unlock()
		return struct{}{}, false
	}

	pending := make(chan interface{}, 1)
	elem := Util.PQElem{
		Pri: seq,
		C: pendingTask{
			seq:  seq,
			op:   op,
			done: pending,
			dgt:  digest,
		},
	}

	Util.Dprintf("%s[AddQueue] %v @ %v", s, op, seq)

	inserted := s.applyQueue.Insert(elem)
	if !inserted {
		panic("Already insert some request with same sequence")
	}

	for i, sz := 0, s.applyQueue.Length(); i < sz; i++ {
		m, err := s.applyQueue.GetMin()
		if err != nil {
			break
		}
		if s.apply+1 == m.Pri {
			s.apply++
			if m.C.(pendingTask).dgt != "" {
				s.state = append(s.state.([]interface{}), m.C.(pendingTask).op)
			}
			m.C.(pendingTask).done <- m.C.(pendingTask).op
			s.applyQueue.ExtractMin()

			Util.Dprintf("%s[Execute] Op:%v @ %v", s, m.C.(pendingTask).op, m.C.(pendingTask).seq)

			if s.apply%checkpointDiv == 0 {
				s.startCheckPoint(s.view, s.apply)
			}
		} else if s.apply+1 > m.Pri {
			panic("This should already done")
		} else {
			break
		}
	}
	s.lock.Unlock()
	return <-pending, true
}

// CheckPoint is the reply of FetchCheckPoint, signature is only set when it transmit by RPC
type CheckPoint struct {
	// lock is unexported, to avoid gob encode this value
	lock   sync.Mutex
	Seq    int
	Stable bool
	State  interface{}
	Proof  []*CheckPointArgs
	// Signature
}

// Should get s.lock before call this function
func (s *PBFTServer) startCheckPoint(v int, n int) {
	cp := s.getCheckPoint(n)
	// There is no newer stable checkpoint
	if cp != nil {
		// This may be a new checkpoint, if so then cp.lock don't have to lock, cause s.lock is locked
		// no other routine can get cp
		// If it's not a new checkpoint, CheckPoint have already been called, but they won't r/w cp.Seq,
		// cp.State. When CheckPoint write cp.stable, it will get s.lock first, so it's safe
		cp.Seq = n
		cp.Stable = false
		// Cause we use a deterministic state machine, so this state must be correct
		cp.State = s.state

		cpArgs := CheckPointArgs{
			Seq:    n,
			Digest: Util.Digest(s.state),
			Rid:    s.id,
		}

		Util.Dprintf("%s[B/CheckPoint]: Seq:%d", s, n)

		s.broadcast(false, true, "PBFTServer.CheckPoint", cpArgs, CheckPointReply{})
	}
}

// CheckPointArgs is the argument for RPC handler PBFTServer.CheckPoint
type CheckPointArgs struct {
	Seq    int
	Digest string
	Rid    int
}

// CheckPointReply is the reply for RPC handler PBFTServer.CheckPoint
type CheckPointReply struct {
}

// CheckPoint is the handler of RPC PBFTServer.CheckPoint
// When a checkpoint is stable, the state of server will at least uptodate to that checkpoint
// cp.lock -> s.lock
func (s *PBFTServer) CheckPoint(args CheckPointArgs, reply *CheckPointReply) error {
	// Verify signature

	s.lock.Lock()
	cp := s.getCheckPoint(args.Seq)
	s.lock.Unlock()

	if cp != nil {
		cp.lock.Lock()

		if !cp.Stable {
			cp.Proof = append(cp.Proof, &args)

			// Util.Dprintf("%s[R/CheckPoint]:Args:%+v,Total:%d", s, args, len(cp.Proof))

			if len(cp.Proof) > 2*s.f {
				count := make(map[string]int)
				for i, sz := 0, len(cp.Proof); i < sz; i++ {
					_, ok := count[cp.Proof[i].Digest]
					if !ok {
						count[cp.Proof[i].Digest] = 1
					} else {
						count[cp.Proof[i].Digest]++
					}

					// mp := ""
					// for q, w := range count {
					// 	mp += fmt.Sprintf("%s-%v;", q, w)
					// }
					// Util.Dprintf("%s[CPCount]:%s", s, mp)

					// Stablize checkpoint
					if count[cp.Proof[i].Digest] > 2*s.f {
						Util.Dprintf("%s[Stablize]:Seq:%d,Digest:%s", s, args.Seq, cp.Proof[i].Digest)
						s.lock.Lock()

						get, ncps := true, args.Seq
						if s.apply < args.Seq {
							// fetState is a sync call, but can't block forever
							get, ncps = s.fetchState(args.Seq)
						}
						if get {
							s.stablizeCP(s.cps[ncps])
						} else {
							panic("Fail to get checkpoint")
						}

						s.lock.Unlock()
						break
					}
				}
			}
		}

		cp.lock.Unlock()
	}

	return nil
}

// Should lock s.lock before call this function
// Trying to fetch newest stable checkpoint from other peer, if success return true
func (s *PBFTServer) fetchState(seq int) (bool, int) {
	Util.Dprintf("%s[StartFetchCP] @ %v", s, seq)
	for i, sz := 0, len(s.replicas); i < sz; i++ {
		if i != s.id {
			cp := CheckPoint{}

			err := s.replicas[i].Call("PBFTServer.FetchCheckPoint", seq, &cp)

			if err == nil && cp.Stable && cp.Seq >= seq && s.verifyCheckPoint(&cp) {
				Util.Dprintf("%s[FetchCP]:Args:%+v", s, cp.Seq)
				// s.lock is already lock, so this is safe
				s.cps[cp.Seq] = &cp
				return true, cp.Seq
			}
		}
	}
	return false, -1
}

// Make sure this is the newest checkpoint before call this function
func (s *PBFTServer) stablizeCP(cp *CheckPoint) {
	Util.Dprintf("%s[Update]:Checkpoint:{%v,%v}", s, cp.Seq, cp.Stable)

	cp.Stable = true

	if s.apply < cp.Seq {
		s.apply = cp.Seq
		s.state = cp.State
	}
	s.removeOldLog(cp.Seq)
	s.removeOldCheckPoint(cp.Seq)
	s.h = cp.Seq
	s.H = s.h + 2*checkpointDiv
	for i, sz := 0, s.applyQueue.Length(); i < sz; i++ {
		m, err := s.applyQueue.GetMin()
		if err != nil {
			break
		}
		if m.Pri <= cp.Seq {
			m, _ = s.applyQueue.ExtractMin()
			m.C.(pendingTask).done <- struct{}{}
		} else {
			break
		}
	}
	s.lastcp = cp.Seq
}

// Verify checkpoint
func (s *PBFTServer) verifyCheckPoint(cp *CheckPoint) bool {
	// Verify signature

	rSet := make(map[int]bool)
	dgt := Util.Digest(cp.State)
	for i, sz := 0, len(cp.Proof); i < sz; i++ {
		if cp.Proof[i].Seq == cp.Seq && cp.Proof[i].Digest == dgt {
			rSet[cp.Proof[i].Rid] = true
		}
	}
	return len(rSet) > 2*s.f
}

// FetchCheckPoint will send the stable checkpoint of this peer, if it didn't have any, do nothing
// RPC client will find out it's an invalid reply, FetchCheckPoint may fetch a newer checkpoint
func (s *PBFTServer) FetchCheckPoint(seq int, reply *CheckPoint) error {
	Util.Dprintf("%s[Fetch]:Args:%v", s, seq)

	s.lock.Lock()
	cp := s.getStableCheckPoint()
	if cp.Seq >= seq {
		*reply = *cp
	} else {
		reply.Stable = false
	}
	s.lock.Unlock()
	return nil
}

func (s *PBFTServer) startViewChange() {
	s.lock.Lock()
	Util.Dprintf("%s[StartViewChange]", s)
	s.monitor = false
	vcArgs := s.generateViewChange()
	s.lock.Unlock()

	Util.Dprintf("%s[B/ViewChange]:Args:%+v", s, *vcArgs)
	// If i didn't add this, it will throw a exception about didn't register []interface{}
	gob.Register(make([]interface{}, 0))
	s.broadcast(false, false, "PBFTServer.ViewChange", *vcArgs, &ViewChangeReply{})
}

// Should lock s.lock before call this function
func (s *PBFTServer) generateViewChange() *ViewChangeArgs {
	s.changing = true
	cp := s.getStableCheckPoint()

	vcArgs := ViewChangeArgs{
		View: s.view + 1,
		Rid:  s.id,
		CP:   cp,
	}

	log := s.log

	for k, v := range log {
		if k.n > cp.Seq && v.pp != nil && (v.sendCommit || s.prepared(v)) {
			pm := Pm{
				PP: v.pp,
				P:  v.p,
			}
			vcArgs.P = append(vcArgs.P, &pm)
		}
	}

	return &vcArgs
}

// Pm is use to hold a preprepare message and at least 2f corresponding prepare message
type Pm struct {
	PP *PrePrepareArgs
	P  []*PrepareArgs
}

// ViewChangeArgs is the argument for RPC handler PBFTServer.ViewChange
type ViewChangeArgs struct {
	View int
	CP   *CheckPoint
	P    []*Pm
	Rid  int
}

// ViewChangeReply is the reply for RPC handler PBFTServer.ViewChage
type ViewChangeReply struct {
}

// ViewChange is the handler of RPC PBFTServer.ViewChange
// TODO : what if there are multiple view change
func (s *PBFTServer) ViewChange(args ViewChangeArgs, reply *ViewChangeReply) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	Util.Dprintf("%s[R/ViewChange]:Args:%+v", s, args)

	// Ignore old viewchange message
	if args.View <= s.view || args.Rid == s.id {
		return nil
	}

	_, ok := s.vcs[args.View]
	if !ok {
		s.vcs[args.View] = make([]*ViewChangeArgs, 0)
	}

	// Insert this view change message to its log
	s.vcs[args.View] = append(s.vcs[args.View], &args)

	// Leader entering new view
	if (args.View%len(s.replicas) == s.id) && len(s.vcs[args.View]) >= 2*s.f {
		nvArgs := NewViewArgs{
			View: args.View,
			V:    s.vcs[args.View],
		}
		nvArgs.V = append(nvArgs.V, s.generateViewChange())

		mins, maxs, pprepared := s.calcMinMaxspp(&nvArgs)
		pps := s.calcPPS(args.View, mins, maxs, pprepared)

		nvArgs.O = pps

		Util.Dprintf("%s[B/NewView]:Args:%+v", s, nvArgs)

		s.broadcast(true, false, "PBFTServer.NewView", nvArgs, NewViewReply{})

		s.enteringNewView(&nvArgs, mins, maxs, pps)
	}

	return nil
}

func (s *PBFTServer) enteringNewView(nvArgs *NewViewArgs, mins int, maxs int, pps []*PrePrepareArgs) []*PrepareArgs {
	Util.Dprintf("%s[EnterNextView]:%v", s, nvArgs.View)

	scp := s.getStableCheckPoint()
	if mins > scp.Seq {
		for i, sz := 0, len(nvArgs.V); i < sz; i++ {
			if nvArgs.V[i].CP.Seq == mins {
				s.cps[mins] = nvArgs.V[i].CP
				break
			}
		}
		s.stablizeCP(s.cps[mins])
	}

	s.tSeq = maxs + 1
	ps := make([]*PrepareArgs, len(pps))
	for i, sz := 0, len(pps); i < sz; i++ {
		cid, cseq := pps[i].Message.Id, pps[i].Message.Seq
		if cseq >= s.seq[cid] {
			s.seq[cid] = cseq + 1
		}
		s.seqmap[entryID{cid, cseq}] = pps[i].Seq
		ent := s.getEntry(entryID{nvArgs.View, pps[i].Seq})
		ent.pp = pps[i]

		pArgs := PrepareArgs{
			View:   nvArgs.View,
			Seq:    pps[i].Seq,
			Digest: pps[i].Digest,
			Rid:    s.id,
		}
		ps[i] = &pArgs
	}
	s.view = nvArgs.View
	s.removeOldLog(mins)
	s.stopTimer()
	s.changing = false
	s.removeOldViewChange(nvArgs.View)
	s.reqQueue = make([]*Util.PriorityQueue, len(s.clients))
	for i, sz := 0, len(s.clients); i < sz; i++ {
		s.reqQueue[i] = Util.NewPriorityQueue()
	}

	go func() {
		s.lock.Lock()
		s.lock.Unlock()
		for i, sz := 0, len(ps); i < sz; i++ {
			Util.Dprintf("%s[B/Prepare]:Args:%+v", s, ps[i])
			s.broadcast(false, true, "PBFTServer.Prepare", *(ps[i]), &PrepareReply{})
			time.Sleep(5 * time.Millisecond)
		}
	}()

	return ps
}

func (s *PBFTServer) calcMinMaxspp(nvArgs *NewViewArgs) (int, int, map[int]*PrePrepareArgs) {
	mins, maxs := -1, -1
	pprepared := make(map[int]*PrePrepareArgs)
	for i, sz := 0, len(nvArgs.V); i < sz; i++ {
		if nvArgs.V[i].CP.Seq > mins {
			mins = nvArgs.V[i].CP.Seq
		}
		for j, psz := 0, len(nvArgs.V[i].P); j < psz; j++ {
			if nvArgs.V[i].P[j].PP.Seq > maxs {
				maxs = nvArgs.V[i].P[j].PP.Seq
			}
			pprepared[nvArgs.V[i].P[j].PP.Seq] = nvArgs.V[i].P[j].PP
		}
	}
	return mins, maxs, pprepared
}

func (s *PBFTServer) calcPPS(view int, mins int, maxs int, pprepared map[int]*PrePrepareArgs) []*PrePrepareArgs {
	pps := make([]*PrePrepareArgs, 0)
	for i := mins + 1; i <= maxs; i++ {
		v, ok := pprepared[i]
		if ok {
			pps = append(pps, &PrePrepareArgs{
				View:    view,
				Seq:     i,
				Digest:  v.Digest,
				Message: v.Message,
			})
		} else {
			pps = append(pps, &PrePrepareArgs{
				View:   view,
				Seq:    i,
				Digest: "",
			})
		}
	}
	return pps
}

// NewViewArgs is the argument for RPC handler PBFTServer.NewView
type NewViewArgs struct {
	View int
	V    []*ViewChangeArgs
	O    []*PrePrepareArgs
}

// NewViewReply is the reply for RPC handler PBFTServer.NewView
type NewViewReply struct {
}

// NewView is the handler of RPC PBFTServer.NewView
func (s *PBFTServer) NewView(args NewViewArgs, reply *NewViewReply) error {
	// Verify signature

	if args.View <= s.view {
		return nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	Util.Dprintf("%s[R/NewView]:Args:%+v", s, args)

	// Verify V sest
	vcs := make(map[int]bool)
	for i, sz := 0, len(args.V); i < sz; i++ {
		if args.V[i].View == args.View {
			vcs[args.V[i].Rid] = true
		}
	}
	if len(vcs) <= 2*s.f {
		Util.Dprintf("%s[V/NewView/Fail] view change message is not enough", s)
		return nil
	}

	// Verify O set
	mins, maxs, pprepared := s.calcMinMaxspp(&args)
	pps := s.calcPPS(args.View, mins, maxs, pprepared)

	for i, sz := 0, len(pps); i < sz; i++ {
		if pps[i].View != args.O[i].View || pps[i].Seq != args.O[i].Seq || pps[i].Digest != args.O[i].Digest {
			Util.Dprintf("%s[V/NewView/Fail] PrePrepare message missmatch : %+v", s, pps[i])
			return nil
		}
	}

	s.enteringNewView(&args, mins, maxs, pps)

	Util.Dprintf("%s[NowInNewView]:%v", s, args.View)

	return nil
}

// Should lock s.lock before call this function
func (s *PBFTServer) getEntry(id entryID) *entry {
	_, ok := s.log[id]
	if !ok {
		s.log[id] = &entry{
			pp:         nil,
			p:          make([]*PrepareArgs, 0),
			sendCommit: false,
			c:          make([]*CommitArgs, 0),
			sendReply:  false,
			r:          nil,
		}
	}
	return s.log[id]
}

// Should lock s.lock before call this function
// When the checkpoint didn't exist then, it must been deleted or it's a new checkpoint
// so this function will also check the hold checkpoint, if there is any checkpoint newer than
// it, it will return nil
func (s *PBFTServer) getCheckPoint(seq int) *CheckPoint {
	_, ok := s.cps[seq]
	if !ok {
		for k, v := range s.cps {
			if k > seq && v.Stable {
				return nil
			}
		}

		s.cps[seq] = &CheckPoint{
			Seq:    seq,
			Stable: false,
			State:  nil,
			Proof:  make([]*CheckPointArgs, 0),
		}
	}
	return s.cps[seq]
}

func (s *PBFTServer) getStableCheckPoint() *CheckPoint {
	for _, v := range s.cps {
		if v.Stable {
			return v
		}
	}
	panic("No stable checkpoint")
}

// Should lock s.lock before call this function
func (s *PBFTServer) removeOldCheckPoint(seq int) {
	for k, v := range s.cps {
		if v.Seq < seq {
			delete(s.cps, k)
		}
	}
}

// Should lock s.lock before call this function
func (s *PBFTServer) removeOldLog(seq int) {
	for k := range s.log {
		if k.n < seq {
			delete(s.log, k)
		}
	}
}

// Should lock s.lock before call this function
func (s *PBFTServer) removeOldViewChange(seq int) {
	for k := range s.vcs {
		if k < seq {
			delete(s.vcs, k)
		}
	}
}

// Should lock s.lock before call this function
func (s *PBFTServer) removeEntry(id entryID) {
	delete(s.log, id)
}

func (s *PBFTServer) stopTimer() {
	if s.change != nil {
		s.change.Stop()
	}
	s.monitor = false
}

func (s *PBFTServer) String() string {
	return fmt.Sprintf("{S:ID:%d,Seq:%d,View:%d,CP:%v,Apply:%v,h:%v,H:%v}", s.id, s.tSeq, s.view, s.lastcp, s.apply, s.h, s.H)
}

func (s *PBFTServer) broadcast(sc bool, toself bool, method string, args interface{}, reply interface{}) error {
	wg := sync.WaitGroup{}
	for i, sz := 0, len(s.replicas); i < sz; i++ {
		if toself || s.id != i {
			if sc {
				wg.Add(1)
			}
			go func(rid int) {
				// reptr := reflect.New(reflect.TypeOf(reply))
				// rep := reptr.Elem().Interface()
				err := s.replicas[rid].Call(method, args, &struct{}{})

				if err != nil {
					Util.Dprintf("%s[RPC/Error]:%s:Args:%+v:Error:%v", s, method, args, err)
				}
				if sc {
					wg.Done()
				}
			}(i)
		}
	}
	wg.Wait()
	return nil
}

// NewPBFTServer use given information create a new pbft server
func NewPBFTServer(rid int, f int, c []*RPC.ClientEnd, r []*RPC.ClientEnd) *PBFTServer {
	s := PBFTServer{
		id:         rid,
		tSeq:       0,
		seq:        make([]int, len(c)),
		seqmap:     make(map[entryID]int),
		view:       0,
		apply:      -1,
		log:        make(map[entryID]*entry),
		cps:        make(map[int]*CheckPoint),
		h:          0,
		H:          2 * checkpointDiv,
		f:          f,
		monitor:    false,
		change:     nil,
		changing:   false,
		clients:    c,
		replicas:   r,
		state:      make([]interface{}, 1),
		applyQueue: Util.NewPriorityQueue(),
		vcs:        make(map[int][]*ViewChangeArgs),
		lastcp:     -1,
		reqQueue:   make([]*Util.PriorityQueue, len(c)),
	}

	for i, sz := 0, len(s.clients); i < sz; i++ {
		s.reqQueue[i] = Util.NewPriorityQueue()
		s.seq[i] = 0
	}

	// Put an initial stable checkpoint
	cp := s.getCheckPoint(-1)
	cp.Stable = true
	cp.State = s.state

	return &s
}
