package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


func RaftPrint(rf *Raft, shouldLog bool, format string, a ...interface{}) (n int, err error) {
	if Debug && shouldLog {
		prefix := fmt.Sprintf("[S%v][%v][T%v][C%v][LL%v]", rf.me, string([]rune(rf.state.toString())[0]), rf.currentTerm, rf.commitIndex, len(rf.log) - 1)
		log.Printf(prefix + " " + format, a...)
	}
	return
}
