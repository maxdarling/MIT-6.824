package raft

// bug log:
// - <1 or 2 forgotten in 2A...>
//
// - 2B:
// - forgot to init apply channel
// - had observePotentiallyHigherTerm on args.Term always, instead of reply.Term for senders
// caused leaders not to step down to runaway stale candidates/followers, which is subtle.
// I was going crazy, assuming that the paper was missing logic for that case. in fact, term
// is the law for demotions, but log up-to-dateness gates elections. simple.
// - typo'ed args.Term instead of args.LastLogTerm in isCandAtLeastAsUpToDate, rough. spent ~1.5hr
// printing stuff and finally tracing output on paper, at which point it became clear where the bug
// lied.
// - my commit scheme was wrong. to make it as simple as possible, I started at the current commit index
// and walked up from there in a loop, exiting if it didn't meet the N criteria. but this totally
// fails and causes commitment to get stuck if there's a log entry with from an earlier term: it fails
// the old term check. the real way is to go backwards to avoid this... illustrated by fig 8. or, I can
// do my scheme, but I need to walk from commitIdx to an entry with term == currentTerm.
//
// 2C

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
	//	"bytes"
	// "log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
// private types. (I wanted to nest these inside the Raft struct but it's impossible?
type state int
const (
	FOLLOWER_STATE state = iota
	CANDIDATE_STATE
	LEADER_STATE

	// broadcast time is X (e.g. 20ms), so timeout can be 10x greater
	ELECTION_TIMEOUT_LOW_MS = 300
	ELECTION_TIMEOUT_HIGH_MS = 500
	HEARTBEAT_INTERVAL_MS = 100 // note: this is the lower bound required by the tester
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
	voted       bool // <added, since below can't be nulled>
	votedFor    int
	log         []LogEntry // first index is 1

	// volatile state on all servers
	commitIndex int
	lastApplied int
	state       state
	isElectionTimeoutFresh bool // todo: fancier and more accurate to refresh actual timestamp
	votesReceived int

	// volatile state on *leaders*
	nextIndex  []int
	matchIndex []int

	// misc
	commitIndexCV sync.Cond // must signal when commitIndex is incremented
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
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

func observePotentiallyHigherTerm(peerTerm int, rf *Raft) {
	if peerTerm > rf.currentTerm {
		if rf.state != FOLLOWER_STATE {
			RaftPrint(rf, true, "demoted after observing higher term %v", peerTerm)
			rf.state = FOLLOWER_STATE
		}
		rf.currentTerm = peerTerm
		rf.voted = false
		rf.votedFor = -1 // maybe not needed...?
	}
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

func (rf *Raft) isCandAtLeastAsUpToDate(args *RequestVoteArgs) bool {
	lastLogEntry := rf.log[len(rf.log) - 1]
	res := (args.LastLogTerm != lastLogEntry.Term && args.LastLogTerm >= lastLogEntry.Term) ||
		(lastLogEntry.Term == args.LastLogTerm && args.LastLogIndex >= len(rf.log) - 1)
	if !res { // debug 
		RaftPrint(rf, true, "--- REJECTING leader, not up to date enough --- ")
	}
	return res
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	defer rf.mu.Unlock()
	rf.mu.Lock()

	observePotentiallyHigherTerm(args.Term, rf)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term >= rf.currentTerm &&
		(!rf.voted || rf.votedFor == args.CandidateId) &&
		rf.isCandAtLeastAsUpToDate(args) {
		reply.VoteGranted = true
		rf.voted = true
		rf.votedFor = args.CandidateId
		rf.isElectionTimeoutFresh = true
	}
}

// send request vote to a peer, wait for their response, and handle the potential outcome
func (rf *Raft) sendRequestVote(server int) {
	rf.mu.Lock()
	lastLogIdx := len(rf.log) - 1
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIdx, rf.log[lastLogIdx].Term}
	reply := RequestVoteReply{}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	defer rf.mu.Unlock()
	rf.mu.Lock()
	if !ok {
		// RaftPrint(rf, true, "error during RequestVote to server %v", server)
		return
	}

	observePotentiallyHigherTerm(reply.Term, rf)

	if rf.state != CANDIDATE_STATE || rf.currentTerm != args.Term {
		// RaftPrint(rf, true, "discarding stale RequestVote response from server %v", server)
		return
	}

	// did they vote for us?
	if reply.VoteGranted {
		rf.votesReceived++
		// RaftPrint(rf, true, "vote received from server %v", server) 
	} else {
		// RaftPrint(rf, true, "vote withheld by server %v", server) 
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

	observePotentiallyHigherTerm(args.Term, rf)
	reply.Term = rf.currentTerm
	rf.isElectionTimeoutFresh = true

	if args.Term < rf.currentTerm {
		RaftPrint(rf, true, "rejecting AE based on term")
		reply.Success = false
		rf.isElectionTimeoutFresh = false
	} else if args.PrevLogIndex >= len(rf.log) ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		RaftPrint(rf, true, "rejecting AE with need to backup (PLI: %v, LL: %v, LE: %v", args.PrevLogIndex, len(rf.log), rf.log[len(rf.log) - 1])
		reply.Success = false
	} else {
		reply.Success = true
		rf.log = rf.log[:args.PrevLogIndex + 1] // remove all non-matching entries
		for _, e := range args.Entries {
			rf.log = append(rf.log, e)
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
	// if rf.state != LEADER_STATE {
	// 	return
	// }
	nextIdx := rf.nextIndex[server]
	lastIdx := len(rf.log) - 1 // idx of last entry to be sent
	heartbeat := false // debug
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
	RaftPrint(rf, !heartbeat, "sending AppendEntries to server %v at NI %v", server, nextIdx)
	reply := AppendEntriesReply{}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	defer rf.mu.Unlock()
	rf.mu.Lock()
	if !ok {
		// RaftPrint(rf, !heartbeat, "error during AppendEntries to server %v", server)
		return
	}

	observePotentiallyHigherTerm(reply.Term, rf)

	if rf.state != LEADER_STATE || rf.currentTerm != args.Term  || rf.matchIndex[server] >= lastIdx {
		RaftPrint(rf, !heartbeat, "discarding stale AppendEntries response from server %v", server)
		return
	}

	// was request successful?
	if !reply.Success {
		// RaftPrint(rf, !heartbeat, "AppendEntries rejected by server %v, retrying...", server)
		rf.nextIndex[server] = max(1, rf.nextIndex[server] - 1)
		go rf.sendAppendEntries(server)
		return
	}

	RaftPrint(rf, !heartbeat, "AppendEntries succeeded for server %v (MI %v -> %v)", server, rf.matchIndex[server], lastIdx)
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
		// RaftPrint(rf, true, "LEADER COMMMITTED! index %v", N)
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

// start a new election if this peer hasn't received heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		timeout := ELECTION_TIMEOUT_LOW_MS +
			rand.Intn(ELECTION_TIMEOUT_HIGH_MS - ELECTION_TIMEOUT_LOW_MS)
		time.Sleep(time.Duration(timeout) * time.Millisecond)

		rf.mu.Lock()
		if rf.state != LEADER_STATE && rf.isElectionTimeoutFresh == false {
			rf.state = CANDIDATE_STATE
			rf.startElection()
		}
		rf.isElectionTimeoutFresh = false
		rf.mu.Unlock()
	}
}

// single go routine to asynchronously apply log entries.
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.commitIndexCV.Wait()
		}

		rf.lastApplied++
		RaftPrint(rf, true, "APPLIED! index %v", rf.lastApplied)
		msg := ApplyMsg{
			CommandValid: true,
			Command: rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		// apply to state machine (no need to hold lock, as we are only applier)
		// (this would matter if it was slow, but in this setting it's super fast)
		rf.mu.Unlock()
		rf.applyCh <- msg
	}
}


// <assumes lock is held>
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.voted = true
	rf.votedFor = rf.me
	rf.isElectionTimeoutFresh = true
	rf.votesReceived = 1
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	RaftPrint(rf, true, "START ELECTION")

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i)
	}
}

// handle leader tasks, e.g. sending heartbeats at intervals.
func (rf *Raft) leaderHandler() {
	for rf.killed() == false {
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
	rf.isElectionTimeoutFresh = false
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.log = append(rf.log, LogEntry{}) // sentinel for 1-indexing
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

// notes:
// - we're using the special labrpc library. pretty automagic and nice. see ../labrpc/labrpc.go
// for more details, or read the below:

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
