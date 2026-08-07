## TODO

[X] 1. What the timer actually does
Every node runs a loop in its own goroutine: wait for a randomized duration (e.g. 150–300ms), and if nothing resets it before it fires, call startElection. This needs to start as soon as a node boots up as a Follower.

[X] 2. Why a plain time.Sleep won't work
A Go timer that can be reset from other goroutines needs a channel-based reset signal, not a plain time.Sleep. The common pattern: a time.Timer plus a resetCh channel that other code sends to whenever the leader is heard from — the timer loop selects on both the timer firing and the reset signal arriving.

[X] 3. Where it gets reset
Right now AppendEntries has a commented-out line: n.resetElectionTimeout(). That's exactly the moment a follower should reset its timer — hearing from a real, current leader is proof the cluster is healthy and no election is needed. This is the wiring you're missing.

[X] 4. Why the timeout must be randomized each time
Randomization is what prevents every follower from timing out at the exact same moment and splitting the vote forever (this came up back when we first discussed leader election). Pick the timeout fresh, randomly, every single time the timer restarts — not once at startup.

[] 5. Where the code goes
Put this in internal/raft/election.go alongside startElection — it's part of the same concern (when and how a node decides to become a candidate).
