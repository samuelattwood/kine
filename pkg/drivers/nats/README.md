# Two-node

Config option

- PeerURL
- PeerLeader

Protocol

Node start up

- Creates a local KV if not already exists
- Assert local health (no-op for demo)
- Attempt to establish initial connection with peer
  - If fail to connect
    - Retry N times before state transition (retry until successful)
      - If designated leader continue
  - Send request to peer for current state
    - `nats req state`
    - `{"health": "ok", "isLeader": true, "latestSeq": 2823}`
- Follower will resume ordered consumer from local latestSeq
  - TODO: do some sanity checking of messages
  - Live connection in Kine will be to the leader

## Demo

### Start leader

- Start watch KV report of leader on startup
  - `watch nats -s localhost:4222 stream report KV_kine`
- Start watch of Kubectl leases
  `export KUBECONFIG=kubeconfig-leader.yaml`
  - `watch kubectl -n kube-system get leases`
- Start leader containers
  - `docker compose up -d nats1 kine1 k3s1`
- Subscribe to NATS status
  - `nats -s localhost:4222 sub status`
  - `{"health": "ok", "isLeader": true}`
- Observe until stabilizes
- Run a container
  - `kubectl apply -f workfloads/nginx.yaml`

### Start follower

- Start watch KV report of follower on startup
  - `watch nats -s localhost:4223 stream report KV_kine`
- Start watch of Kubectl leases
  `export KUBECONFIG=kubeconfig-follower.yaml`
  - `watch kubectl get pods`
- Start follower containers
  - `docker compose up -d nats2 kine2 k3s2`
- Subscribe to NATS status
  - `nats -s localhost:4223 sub status`
  - `{"health": "ok", "isLeader": false}`
- Observe until stabilizes

### Kill follower

- Stop the follower containers
  - `docker compose stop nats2 kine2 k3s2`
- Observe the leader contines to work fine
  - Look at the messages in the stream
- Start follower again
  - `docker compose stop nats2 kine2 k3s2`
- Look how the messages catch-up

### Swap leader

- Signal leader to step-down
  - `nats -s localhost:4222 stepdown`
- Observe the subscriptions to status change

### Kill leader

- Stop the follower containers
  - `docker compose stop nats1 kine1 k3s1`
- Observe the leader contines to work fine
  - Look at the messages in the stream
- Start follower again
  - `docker compose stop nats1 kine1 k3s1`
- Look how the messages catch-up
