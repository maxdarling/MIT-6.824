## Project 1 - Map Reduce
✔ all tests passing

**To run (macOS)**
From `src/main`...
- run the test suite with `bash test-mr.sh`
- Alternatively, run a coordinator and workers (e.g. on the `wc.go` app) via the following:
```
# coordinator (word-count app)
(rm -f mr-out-* ||:) && go build -race -buildmode=plugin ../mrapps/wc.go && echo "done building" && go run -race mrcoordinator.go pg-*.txt
# worker
go run -race mrworker.go wc.so
```

## Project 2 - Raft
