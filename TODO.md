# TODO

- [x] **1. What the timer actually does**
      Every node runs a loop in its own goroutine: wait for a randomized duration (e.g. 150-300ms), and if nothing resets it before it fires, call `startElection`. This needs to start as soon as a node boots up as a Follower.

- [x] **2. Why a plain time.Sleep won't work**
      A Go timer that can be reset from other goroutines needs a channel-based reset signal, not a plain `time.Sleep`. The common pattern: a `time.Timer` plus a `resetCh` channel that other code sends to whenever the leader is heard from, where the timer loop selects on both the timer firing and the reset signal arriving.

- [x] **3. Where it gets reset**
      Right now `AppendEntries` has a commented-out line: `n.resetElectionTimeout()`. That's exactly the moment a follower should reset its timer: hearing from a real, current leader is proof the cluster is healthy and no election is needed. This is the wiring you're missing.

- [x] **4. Why the timeout must be randomized each time**
      Randomization is what prevents every follower from timing out at the exact same moment and splitting the vote forever (this came up back when we first discussed leader election). Pick the timeout fresh, randomly, every single time the timer restarts, not once at startup.

- [x] **5. Where the code goes**
      Put this in `internal/raft/election.go` alongside `startElection` as it's part of the same concern (when and how a node decides to become a candidate).

- [x] **6. Transport**
      Implement a fake transport with the following struct:

  ```go
  type MockTransport struct {
  	nodes map[string]*Node
  }
  ```

  Methods included:
  - `Register(peerId string, node *Node)`
  - `SendRequestVote(peerId string, args *RequestVoteArgs)`
  - `SendAppendEntries(peerId string, args *AppendEntriesArgs)`

- [x] **7. A heartbeat loop, started in becomeLeader**
      Once a node calls becomeLeader, it needs a background goroutine that fires every ~50-100ms (well under the 150-300ms election timeout) for as long as this node remains leader. Each tick sends an AppendEntries to every peer, with an empty Entries slice for now (real log replication comes after this).

- [x] **8. Stopping when no longer leader**
      The heartbeat goroutine needs to know when to stop — specifically, the moment this node discovers a higher term and steps down to Follower (which already happens inside startElection's vote-counting code when reply.Term > n.currentTerm). A dedicated stop channel (separate from the node's overall stopCh) works well here.

- [x] **9. Wire SendAppendEntries into MockTransport**
      AppendEntries needs to be added to your Transport/MockTransport, mirroring how SendRequestVote already works — route the call to the target node's AppendEntries method directly.

- [x] **10. Add heartbeat logging to see it working**
      With heartbeats running, add a log line each time a follower receives one (or each time a leader sends one) so you can watch the cluster settle into a stable state — one leader, two followers, heartbeats flowing, no more elections firing.
