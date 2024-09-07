package raft

//////////////////////
// BUG LOG
//////////////////////
// 2A:
// - (maybe 1 or 2, I forgot)
//
// 2B:
// - forgot to init apply channel
// - had observePotentiallyHigherTerm on args.Term always, instead of reply.Term for senders
// caused leaders not to step down to runaway stale candidates/followers, which is subtle.
// I was going crazy, assuming that the paper was missing logic for that case. in fact, term
// is the law for demotions, but log up-to-dateness gates elections. simple.
// - typo'ed args.Term instead of args.LastLogTerm in isCandAtLeastAsUpToDate, rough. spent ~1.5hr
// printing stuff and finally tracing output on paper, at which point it became clear where the bug
// lied.
// - nextIndex would back up behind 0 with a simple decrement scheme if messages get retried. need
// to lower bound to 1.
// - my commit scheme was wrong. to make it as simple as possible, I started at the current commit index
// and walked up from there in a loop, exiting if it didn't meet the N criteria. but this totally
// fails and causes commitment to get stuck if there's a log entry from an earlier term: it fails
// the old term check. the real way is to go backwards to avoid this... illustrated by fig 8. or, I can
// do my scheme, but I need to walk from commitIdx to an entry with term == currentTerm.
// - to simplify code, in AE receive logic in the success case, I had `rf.log = rf.log[:args.PrevLogIndex + 1]
// the thought being: if there's new entries, great, we get em, and if there isn't, then it's a no-op.
// the issue, though, is that the leader can send 2 AE in quick succession, one with 1 extra entry,
// and you reply to them out of order (more current one with extra entry first). so with this
// logic my truncation will actually remove the final entry. this is terrible though, since we told
// the leader that we had that entry. so we're in a loop asking for the final entry, but the leader
// rightly thinks we have it. brutal! (I only found this after running 2C tests, then going back
// and running 2B tests many more times. it happened ~1/100 times on TestConcurrentStarts2B)
//
// 2C:
// - OBO on the sendAE backup logic (wouldn't allow tryIdx to backup before XIndex. silly.)
// - typo on the sendAE backup logic (checked XLen == -1 instead of XTerm == -1. oops!). surprisingly,
// i was only failing the figure 8 and figure 8 unreliable tests at ~50% despite these major issues.
// kinda surprising these didn't get caught by any 2B tests. but this was easy to debug (i spotted
// the typo, and the other had logs showing "AE rejected. tried PLI 54, now trying 54")
// - missing check whether we are still leader at the start of sendAE. see code for notes. very
// subtle (but also a classic) 
//////////////////////
// END BUG LOG
//////////////////////

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
// private types. (I wanted to nest these inside the Raft struct but it's impossible in Go?
type state int
const (
	FOLLOWER_STATE state = iota
	CANDIDATE_STATE
	LEADER_STATE

	// broadcast time is X (e.g. 20ms), so timeout can be 10x greater
	ELECTION_TIMEOUT_LOW_MS = 300
	ELECTION_TIMEOUT_HIGH_MS = 500
	HEARTBEAT_INTERVAL_MS = 100 // this is the lower bound accepted by the tester
)
func (s state) toString() string {
	switch s {
	case FOLLOWER_STATE:
		return "FOLLOWER"
	case CANDIDATE_STATE:
		return "CANDIDATE"
	case LEADER_STATE:
		return "LEADER"
	}
	return "?"
}

type LogEntry struct {
	Term int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh chan ApplyMsg         // channel for replies to client

	// persistent state on all servers
	currentTerm int
	voted       bool
	votedFor    int
	log         []LogEntry // 1-indexed (as prescribed in the paper, i.e. init match index to 0)

	// volatile state on all servers
	commitIndex int
	lastApplied int
	state       state
	electionTimeoutRefreshedAt time.Time
	votesReceived int

	// volatile state on *leaders*
	nextIndex  []int
	matchIndex []int

	// misc
	commitIndexCV sync.Cond // tracks 'commitIndex'
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	defer rf.mu.Unlock()
	rf.mu.Lock()
	return rf.currentTerm, rf.state == LEADER_STATE
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voted)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state (if any)
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var voted bool
	var votedFor int
	var rflog []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voted) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rflog) != nil {
		log.Fatal("failed to restore from disk!")
	} else {
		rf.currentTerm = currentTerm
		rf.voted = voted
		rf.votedFor = votedFor
		rf.log = rflog
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool

	// 2C - optimization to backup nextIndex faster
	XTerm  int // term in conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // log length
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// helper. returns true if persistent state changed.
func observePotentiallyHigherTerm(peerTerm int, rf *Raft) bool {
	if peerTerm > rf.currentTerm {
		if rf.state != FOLLOWER_STATE {
			RaftPrint(rf, true, "demoted after observing higher term %v", peerTerm)
			rf.state = FOLLOWER_STATE
		}
		rf.currentTerm = peerTerm
		rf.voted = false
		rf.votedFor = -1
		return true
	}
	return false
}

func (rf *Raft) isCandAtLeastAsUpToDate(args *RequestVoteArgs) bool {
	lastLogEntry := rf.log[len(rf.log) - 1]
	res := (args.LastLogTerm != lastLogEntry.Term && args.LastLogTerm >= lastLogEntry.Term) ||
		(lastLogEntry.Term == args.LastLogTerm && args.LastLogIndex >= len(rf.log) - 1)
	if !res { // debug 
		RaftPrint(rf, true, "--- REJECTING leader, log not up to date enough --- ")
	}
	return res
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer rf.mu.Unlock()
	rf.mu.Lock()
	shouldPersist := false
	defer func() {
		if shouldPersist {
			rf.persist()
		}
	}()

	shouldPersist = shouldPersist || observePotentiallyHigherTerm(args.Term, rf)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term >= rf.currentTerm &&
		(!rf.voted || rf.votedFor == args.CandidateId) &&
		rf.isCandAtLeastAsUpToDate(args) {
		reply.VoteGranted = true
		rf.voted = true
		rf.votedFor = args.CandidateId
		rf.electionTimeoutRefreshedAt = time.Now()
		shouldPersist = true
		RaftPrint(rf, true, "granted vote to S%v", args.CandidateId)
	}
}

// send request vote to a peer, wait for their response, and handle the potential outcome
func (rf *Raft) sendRequestVote(server int) {
	rf.mu.Lock()
	// note: the state of the world might have changed since this goroutine was launched
	// (i.e. our term has increased or our state has changed to leader or candidate)
	// but the follower reply logic is robust enough to handle these cases - namely, due
	// to the guarantee that followers only vote once, and the check on log up-to-date-ness.
	// so, unlike sendAppendEntries, we don't need to do any checks here.

	lastLogIdx := len(rf.log) - 1
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIdx, rf.log[lastLogIdx].Term}
	reply := RequestVoteReply{}

	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	defer rf.mu.Unlock()
	rf.mu.Lock()
	if !ok {
		// RaftPrint(rf, true, "error during RequestVote to S%v", server)
		return
	}

	if observePotentiallyHigherTerm(reply.Term, rf) {
		rf.persist()
	}

	if rf.state != CANDIDATE_STATE || rf.currentTerm != args.Term {
		// RaftPrint(rf, true, "discarding stale RequestVote response from S%v", server)
		return
	}

	// did they vote for us?
	if reply.VoteGranted {
		rf.votesReceived++
		// RaftPrint(rf, true, "vote received from S%v", server) 
	} else {
		// RaftPrint(rf, true, "vote withheld by S%v", server) 
	}

	// should we promote to leader?
	if rf.votesReceived > len(rf.peers) / 2 {
		rf.state = LEADER_STATE
		RaftPrint(rf, true, "WON ELECTION (CI: %v, LL: %v, LLE: %v", rf.commitIndex, len(rf.log), rf.log[len(rf.log) - 1])

		// send heartbeats upon election (per leader instructions). just an optimization?
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.sendAppendEntries(i)
		}
	}
}

// AppendEntries RPC handler
func (rf* Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer rf.mu.Unlock()
	rf.mu.Lock()
	shouldPersist := false
	defer func() {
		if shouldPersist {
			rf.persist()
		}
	}()

	shouldPersist = shouldPersist || observePotentiallyHigherTerm(args.Term, rf)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		RaftPrint(rf, true, "rejecting AE from S%v based on term", args.LeaderId)
		reply.Success = false
	} else if args.PrevLogIndex >= len(rf.log) ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		rf.electionTimeoutRefreshedAt = time.Now()
		// nextIndex faster backup
		if args.PrevLogIndex >= len(rf.log) {
			reply.XTerm = -1
			reply.XLen = len(rf.log)
		} else {
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			xIdx := args.PrevLogIndex
			for ;xIdx > 1 && rf.log[xIdx - 1].Term == reply.XTerm; xIdx-- {
			}
			reply.XIndex = xIdx
		}
		RaftPrint(rf, true, "rejecting AE from S%v with need to backup (PLI: %v, PLT: %v, LL: %v, XT: %v, XI: %v)", args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, len(rf.log), reply.XTerm, reply.XIndex)
	} else {
		reply.Success = true
		rf.electionTimeoutRefreshedAt = time.Now()
		startingLogLen := len(rf.log)

		// subtle: ensure that there really ARE new entries (a delayed AE may come that
		// satisfies the prevLogTerm condition and gets here, but is missing new log entries
		// that we already have. don't naively update the log in that case!
		// but you also can't just do a length check, since the follower's log can be longer
		// than the leaders (with non-commited entries from an earlier term). so you actually
		// have to walk the log from prevLogIndex + 1 till the point where you're missing
		// what the leader is sending you...wild.

		// append entries
		// only modify log if there's new entries (e.g. not heartbeats or stale requests)
		tryIdx := args.PrevLogIndex + 1
		for _, e := range args.Entries {
			// loop invariant: logs match on indices [0, tryIdx)
			if tryIdx == len(rf.log) {
				rf.log = append(rf.log, e)
			} else if rf.log[tryIdx].Term != e.Term {
				rf.log = rf.log[:tryIdx]
				rf.log = append(rf.log, e)
			}

			tryIdx++
		}

		// former simple + buggy code
		// rf.log = rf.log[:args.PrevLogIndex + 1] // remove all non-matching entries
		// for _, e := range args.Entries {
		// 	rf.log = append(rf.log, e)
		// }

		if len(rf.log) > startingLogLen {
			shouldPersist = true
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex + len(args.Entries))
			rf.commitIndex = args.LeaderCommit
			rf.commitIndexCV.Signal()
			RaftPrint(rf, true, "FOLLOWER COMMMITTED! index %v", rf.commitIndex)
		}
	}
}

// send append entries to a peer, wait for response, and handle outcome
func (rf* Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	if rf.state != LEADER_STATE {
		RaftPrint(rf, true, "BUG PREVENTION! Not sending an invalid follower AE!")
		rf.mu.Unlock()
		return
		// subtle: it's possible to send an invalid AE here if we don't do this check.
		// scerario:
		// 1. we are a stale leader with a lower term and a divergent log, and we launch
		// some AE goroutines.
		// 2. we observe a higher term from someone else. we demote to follower and update
		// our term. our log, however, is not yet synced.
		// 3. a previously-launched AE goroutine begins and we send an AE -- our term is
		// up-to-date but our log is divergent! 
		//
		// lesson: in a concurrent program, the state of the world can change in between
		// non-atomic sections. in this case you can't assume the state when 
		// launching the sendAppendEntries call (leader) will be the same when it is
		// eventually executed (a demoted follower). I correctly made this observation
		// in the second locked section after observing the RPC response, but forgot
		// about the first section.
	}
	
	nextIdx := rf.nextIndex[server]
	lastIdx := len(rf.log) - 1 // idx of last entry to be sent
	heartbeat := false // for debugging
	if nextIdx == len(rf.log) {
		heartbeat = true
	}
	args := AppendEntriesArgs{
		rf.currentTerm,
		rf.me,
		nextIdx - 1,
		rf.log[nextIdx - 1].Term,
		rf.log[nextIdx:],
		rf.commitIndex,
	}
	reply := AppendEntriesReply{}

	// RaftPrint(rf, !heartbeat, "sending AE to S%v at NI %v", server, nextIdx)
	rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	defer rf.mu.Unlock()
	rf.mu.Lock()
	if !ok {
		// RaftPrint(rf, !heartbeat, "error during AE to S%v", server)
		return
	}

	if observePotentiallyHigherTerm(reply.Term, rf) {
		rf.persist()
	}

	if rf.state != LEADER_STATE || rf.currentTerm != args.Term  || rf.matchIndex[server] >= lastIdx {
		RaftPrint(rf, !heartbeat, "discarding stale AE response from S%v", server)
		return
	}

	// was request successful?
	if !reply.Success {
		rf.nextIndex[server] = max(1, rf.nextIndex[server] - 1)
		if reply.XTerm == -1 {
			rf.nextIndex[server] = reply.XLen
		} else {
			// the range of disagreement is [XIdx, PLI]
			// we want to walk back through this range, finding the last entry with
			// matching terms, or, if none exist, XIdx - 1 (to start a new range)
			// also if the follower's log is too short, we backup to that point
			tryIdx := args.PrevLogIndex - 1 //
			// tryIdx := args.PrevLogIndex // bottom condition was the bug, but this is
			// good too (redundant to start checking the same thing that got rejected)
			// for ; tryIdx > reply.XIndex && rf.log[tryIdx].Term != reply.XTerm; tryIdx-- {
			for ; tryIdx >= reply.XIndex && rf.log[tryIdx].Term != reply.XTerm; tryIdx-- {
			}
			rf.nextIndex[server] = tryIdx + 1
		}
		RaftPrint(rf, true, "AE rejected by S%v. tried PLI %v, now trying %v", server, args.PrevLogIndex, rf.nextIndex[server] - 1)
		go rf.sendAppendEntries(server)
		return
	}

	RaftPrint(rf, !heartbeat, "AE succeeded for S%v (MI %v -> %v)", server, rf.matchIndex[server], lastIdx)
	rf.nextIndex[server] = lastIdx + 1
	rf.matchIndex[server] = lastIdx

	// walk backwards and commit first thing we can (greedy style)
	for N := len(rf.log) - 1; N > rf.commitIndex && rf.log[N].Term >= rf.currentTerm; N-- {
		if rf.log[N].Term != rf.currentTerm { // can this happen?? I think so.
			continue
		}
		tallies := 0
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				tallies++
			}
		}
		if tallies >= len(rf.peers) / 2 {
			rf.commitIndex = N
			rf.commitIndexCV.Broadcast()
			RaftPrint(rf, true, "LEADER COMMMITTED! index %v", N)
			break
		}
	}
	// RaftPrint(rf, true, "nothing to commit (try: %v, MI: %v, tally: %v, term: %v)", tryIdx, rf.matchIndex, tallies, rf.log[tryIdx].Term)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is the leader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	defer rf.mu.Unlock()
	rf.mu.Lock()
	isLeader := rf.state == LEADER_STATE
	if !isLeader {
		return -1, -1, false
	}
	index := len(rf.log)
	term := rf.currentTerm
	RaftPrint(rf, true, "CLIENT REQUEST -> SENDING AE")

	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	rf.persist()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i)
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// goroutine to handle election timeouts 
func (rf *Raft) ticker() {
	rf.mu.Lock()
	lastRefresh := rf.electionTimeoutRefreshedAt
	rf.mu.Unlock()

	for !rf.killed() {
		timeout := ELECTION_TIMEOUT_LOW_MS +
			rand.Intn(ELECTION_TIMEOUT_HIGH_MS - ELECTION_TIMEOUT_LOW_MS)
		sleepUntil := lastRefresh.Add(time.Duration(timeout) * time.Millisecond)
		time.Sleep(sleepUntil.Sub(time.Now())) // sleeps 0 if neg duration

		rf.mu.Lock()
		if rf.state != LEADER_STATE && rf.electionTimeoutRefreshedAt == lastRefresh {
			rf.state = CANDIDATE_STATE
			rf.startElection()
		}
		lastRefresh = time.Now()
		rf.electionTimeoutRefreshedAt = lastRefresh
		rf.mu.Unlock()
	}
}

// <assumes lock is held>
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.voted = true
	rf.votedFor = rf.me
	rf.votesReceived = 1
	rf.persist()
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	RaftPrint(rf, true, "STARTED ELECTION")

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i)
	}
}

// single goroutine to asynchronously apply log entries.
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.commitIndexCV.Wait()
		}

		rf.lastApplied++
		RaftPrint(rf, true, "APPLIED! %v at index %v", rf.log[rf.lastApplied].Command, rf.lastApplied)
		msg := ApplyMsg{
			CommandValid: true,
			Command: rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.mu.Unlock()
		// no need to hold lock to serialize application, as only this goroutine does applies
		rf.applyCh <- msg 
	}
}

// goroutine to handle leader tasks, e.g. sending heartbeats at intervals.
func (rf *Raft) leaderHandler() {
	for !rf.killed() {
		time.Sleep(time.Duration(HEARTBEAT_INTERVAL_MS) * time.Millisecond)

		rf.mu.Lock()
		if rf.state == LEADER_STATE {
			RaftPrint(rf, false, "Leader sending heartbeats")
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.sendAppendEntries(i)
			}
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voted = false
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER_STATE 
	rf.electionTimeoutRefreshedAt = time.Now()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.log = append(rf.log, LogEntry{}) // log is 1-indexed
	rf.commitIndexCV = *sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start leader goroutine to handle leader tasks
	go rf.leaderHandler()

	// start applier goroutine to handle applying log entries
	go rf.applier()

	return rf
}

// below: notes on the labrpc library. kept for posterity.

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }
