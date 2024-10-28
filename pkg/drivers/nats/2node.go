package nats

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
	"github.com/synadia-labs/tne"
	"github.com/synadia-labs/tne/keys"
)

func jitterSleep(d time.Duration) {
	time.Sleep(d + time.Duration(rand.Intn(100))*time.Millisecond)
}

type Manager struct {
	LocalURL  string
	LocalOpts []nats.Option

	KVConfig *jetstream.KeyValueConfig

	PeerURL  string
	PeerOpts []nats.Option

	Logger *logrus.Logger

	TNEConfig string

	// tne instance.
	tne *tne.TNE

	// Mutex to protect the state.
	mu sync.RWMutex

	// Local references.
	lnc  *nats.Conn
	ljs  jetstream.JetStream
	lkv  jetstream.KeyValue
	lkkv *KeyValue

	// Peer references.
	pnc  *nats.Conn
	pjs  jetstream.JetStream
	pkv  jetstream.KeyValue
	pkkv *KeyValue

	// If true, indicates the local node is the leader.
	isLeader        bool
	leadershipState keys.LeadershipState

	ctx    context.Context
	cancel context.CancelFunc

	// Consumer context to the peer stream when replicating.
	cctx jetstream.ConsumeContext
}

func (m *Manager) GetState() keys.LeadershipState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.leadershipState
}

// initLocalBucket initializes the local key-value bucket. If `seq` is provided,
// the bucket is re-created starting at the given sequence number. This implies
// the bucket is being re-initialized with historical deletes. If `del` is true,
// the bucket is deleted before being re-created. This occurs when the local
// node is transitioning to the follower and the bucket will re-copy the data
// from the leader.
func (m *Manager) initLocalBucket(ctx context.Context, seq uint64, del bool) (jetstream.Stream, error) {
	m.Logger.Infof("init-local: checking if bucket %s exists", m.KVConfig.Bucket)

	var exists bool
	kv, err := m.ljs.KeyValue(ctx, m.KVConfig.Bucket)
	if err == nil {
		exists = true
	} else if errors.Is(err, jetstream.ErrBucketNotFound) {
		exists = false
	} else {
		return nil, fmt.Errorf("init-local: failed to get KV: %w", err)
	}

	// Cancel the previous KV instance to stop the watchers.
	if m.lkkv != nil {
		m.lkkv.Stop()
	}

	// Ensure references are setup.
	if del && exists {
		err := m.ljs.DeleteKeyValue(ctx, m.KVConfig.Bucket)
		if err != nil {
			m.Logger.Errorf("init-local: failed to delete bucket %s: %s", m.KVConfig.Bucket, err)
		}
		exists = false
	}

	if !exists {
		m.Logger.Infof("info-local: creating bucket %s with sequence %d", m.KVConfig.Bucket, seq)

		cfg := jetstream.StreamConfig{
			Name:              fmt.Sprintf("KV_%s", m.KVConfig.Bucket),
			Description:       m.KVConfig.Description,
			Subjects:          []string{fmt.Sprintf("$KV.%s.>", m.KVConfig.Bucket)},
			Storage:           jetstream.FileStorage,
			Replicas:          m.KVConfig.Replicas,
			MaxMsgsPerSubject: int64(m.KVConfig.History),
			AllowRollup:       true,
			DenyDelete:        false,
			AllowDirect:       true,
			FirstSeq:          seq,
			MaxBytes:          -1,
			MaxMsgSize:        -1,
			MaxMsgs:           -1,
			MaxConsumers:      -1,
		}

		_, err := m.ljs.CreateStream(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("init-local: failed to create stream: %w", err)
		}

		kv, err = m.ljs.KeyValue(ctx, m.KVConfig.Bucket)
		if err != nil {
			return nil, fmt.Errorf("init-local: failed to get KV: %w", err)
		}
	}

	m.lkv = kv
	m.lkkv = NewKeyValue(ctx, "local", m.lkv, m.ljs, int(m.KVConfig.History))
	return m.ljs.Stream(ctx, fmt.Sprintf("KV_%s", m.KVConfig.Bucket))
}

// startLocal initializes the local connection and ensures the key-value bucket exists.
func (m *Manager) startLocal(ctx context.Context) error {
	m.stopLocal()

	opts := append([]nats.Option{}, m.LocalOpts...)
	opts = append(opts,
		nats.MaxReconnects(-1),
		nats.Name("kine-local"),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logrus.Errorf("kine-local: nats disconnected: %s", err)
		}),
		nats.DiscoveredServersHandler(func(nc *nats.Conn) {
			logrus.Infof("kine-local: nats discovered servers: %v", nc.Servers())
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			logrus.Errorf("kine-local: nats error callback: %s", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logrus.Infof("kine-local: nats reconnected: %v", nc.ConnectedUrl())
		}),
	)
	// Local connection should always succeed.
	nc, err := nats.Connect(m.LocalURL, opts...)
	if err != nil {
		return fmt.Errorf("init-local: failed to create local connection: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("init-local: failed to get JetStream context: %w", err)
	}

	m.lnc = nc
	m.ljs = js

	// Create the bucket if it doesn't exist. Note, this is a no-op if the bucket
	// already exists with the same configuration.
	if _, err := m.initLocalBucket(ctx, 0, false); err != nil {
		return err
	}
	m.Logger.Infof("init-local: bucket initialized: %s", m.KVConfig.Bucket)

	return nil
}

// startPeer initializes the peer connection and ensures the key-value bucket exists.
func (m *Manager) startPeer(ctx context.Context) {
	m.stopPeer()

	opts := append([]nats.Option{}, m.PeerOpts...)
	opts = append(opts,
		nats.MaxReconnects(-1),
		nats.Name("kine-peer"),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logrus.Errorf("kine-peer: nats disconnected: %s", err)
		}),
		nats.DiscoveredServersHandler(func(nc *nats.Conn) {
			logrus.Infof("kine-peer: nats discovered servers: %v", nc.Servers())
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			logrus.Errorf("kine-peer: nats error callback: %s", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logrus.Infof("kine-peer: nats reconnected: %v", nc.ConnectedUrl())
		}),
	)

	var err error

	i := 0
	var nc *nats.Conn
	for {
		nc, err = nats.Connect(m.PeerURL, opts...)
		if err != nil {
			i++
			m.Logger.Warnf("init-peer: failed to connect: attempt %d: %s", i, err)
			jitterSleep(time.Second)
			continue
		}
		break
	}

	i = 0
	var js jetstream.JetStream
	for {
		js, err = jetstream.New(nc)
		if err != nil {
			i++
			m.Logger.Errorf("init-peer: failed to get JetStream context: attempt %d: %s", i, err)
			jitterSleep(time.Second)
			continue
		}
		break
	}

	i = 0
	var kv jetstream.KeyValue
	for {
		kv, err = js.KeyValue(ctx, m.KVConfig.Bucket)
		if err != nil {
			i++
			m.Logger.Warnf("init-peer: failed to get KV: attempt %d: %s", i, err)
			jitterSleep(time.Second)
			continue
		}
		break
	}

	m.pnc = nc
	m.pjs = js
	m.pkv = kv
	m.pkkv = NewKeyValue(ctx, "peer", kv, js, int(m.KVConfig.History))

	m.Logger.Infof("init-peer: connected to peer: %s", m.PeerURL)
}

// getLocalLastSeq returns the last sequence number for the local bucket.
func (m *Manager) getLocalLastSeq(ctx context.Context) (uint64, error) {
	str, err := m.ljs.Stream(ctx, fmt.Sprintf("KV_%s", m.KVConfig.Bucket))
	if err != nil {
		return 0, fmt.Errorf("failed to get bucket status: %w", err)
	}
	return str.CachedInfo().State.LastSeq, nil
}

// stopStreamReplication stops the replication of the peer stream to the local stream.
func (m *Manager) stopStreamReplication() {
	if m.cctx != nil {
		m.Logger.Infof("stopping stream replication")
		m.cctx.Drain()
		<-m.cctx.Closed()
		m.cctx = nil
	}
}

// startStreamReplication starts replicating the peer stream to the local stream.
func (m *Manager) startStreamReplication(ctx context.Context, done chan error) {
	// Ensure any previous replication is stopped.
	m.stopStreamReplication()

	sname := fmt.Sprintf("KV_%s", m.KVConfig.Bucket)
	con, err := m.pjs.OrderedConsumer(ctx, sname, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		done <- fmt.Errorf("failed to create consumer: %w", err)
		return
	}
	// Indicates when we have caught up with the peer.
	numPending := con.CachedInfo().NumPending
	numRemaining := numPending
	t0 := time.Now()

	// Keep track of the last sequence number that was published.
	seq := uint64(0)
	pstr, err := m.pjs.Stream(ctx, sname)
	if err != nil {
		done <- fmt.Errorf("failed to get peer stream: %w", err)
		return
	}

	// Initial first seq. This will be caught and gaps filled in the message handler.
	firstSeq := pstr.CachedInfo().State.FirstSeq
	if firstSeq > 0 {
		seq = firstSeq - 1
	}
	logrus.Debugf("stream replication starting @ %d", seq)

	lstr, err := m.initLocalBucket(ctx, seq, true)
	if err != nil {
		done <- fmt.Errorf("failed to initialize local bucket: %w", err)
		return
	}

	//var str jetstream.Stream
	msgHandler := func(msg jetstream.Msg) {
		md, _ := msg.Metadata()

		// Compare the last sequence we published to the replicated message.
		// If they don't match, this indicates a gap due to interior deletes
		// or expired messages. We need to publish tombstones for the missing
		// sequence numbers.
		// For example, if the local sequence is 10 and the replicated message
		// sequence is 15, then we need to publish tombstones for 11-14.
		if md.Sequence.Stream != seq+1 {
			if md.Sequence.Stream <= seq {
				panic(fmt.Sprintf("sequence regression: %d -> %d", seq, md.Sequence.Stream))
			}

			m.Logger.Debugf("gap detected: %d -> %d", seq, md.Sequence.Stream)
			num := int(md.Sequence.Stream-seq) - 1

			for i := 0; i < num; i++ {
				subject := fmt.Sprintf("$KV.%s._T", m.KVConfig.Bucket)
				pa, err := m.ljs.Publish(ctx, subject, nil)
				if err != nil {
					m.Logger.Debugf("failed to publish tombstone: %s", err)
					continue
				}

				err = lstr.DeleteMsg(ctx, pa.Sequence)
				if err != nil {
					m.Logger.Debugf("failed to delete tombstone @ %d: %s", i, err)
				}
			}
		}

		nmsg := nats.NewMsg(msg.Subject())
		nmsg.Data = msg.Data()

		hdrs := msg.Headers()
		if len(hdrs) > 0 {
			// Delete the expected last sequence header since that will result in
			// the local server enforcing it which would not work for the first message
			// of each individual subject (since there is no existing history). Instead,
			// for posterity, we'll store it in a separate header suffixed with -Peer.
			expHdr := hdrs.Get(nats.ExpectedLastSubjSeqHdr)
			hdrs.Del(nats.ExpectedLastSubjSeqHdr)
			hdrs.Set(fmt.Sprintf("%s-Peer", nats.ExpectedLastSubjSeqHdr), expHdr)
			nmsg.Header = hdrs
		}

		pa, err := m.ljs.PublishMsg(ctx, nmsg)
		if err != nil {
			//m.Logger.Warnf("failed to publish local message: %s", err)
			panic(fmt.Sprintf("failed to publish local message: %s", err))
		}

		seq = pa.Sequence
		if pa.Sequence != md.Sequence.Stream {
			panic(fmt.Sprintf("sequence mismatch: %d -> %d", md.Sequence.Stream, pa.Sequence))
		}

		numRemaining--
		if numRemaining == 0 {
			m.Logger.Infof("stream caught-up: %d messages in %s", numPending, time.Since(t0))
			done <- nil
		}
	}

	cctx, err := con.Consume(msgHandler, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		// TODO: handle error?
		m.Logger.Warnf("replication consumer error @ seq %d: %s", seq, err)
	}))
	if err != nil {
		done <- fmt.Errorf("failed to start replication consumer: %w", err)
		return
	}

	m.cctx = cctx
	m.Logger.Infof("started stream replication")
}

func (m *Manager) stopPeer() {
	if m.pnc == nil {
		return
	}

	m.pkkv.Stop()
	m.pkkv = nil
	m.pnc.Drain()
	for m.pnc.IsDraining() {
		time.Sleep(10 * time.Millisecond)
	}
	m.pnc = nil
	m.pjs = nil
	m.pkv = nil
}

func (m *Manager) stopLocal() {
	if m.lnc == nil {
		return
	}

	m.lkkv.Stop()
	m.lkkv = nil
	m.lnc.Drain()
	for m.lnc.IsDraining() {
		time.Sleep(10 * time.Millisecond)
	}
	m.lnc = nil
	m.ljs = nil
	m.lkv = nil
}

func (m *Manager) handleStateChanges(ctx context.Context, lch <-chan keys.LeadershipKVState, tch <-chan keys.TransitioningKVState) {
	for {
		select {
		case ls := <-lch:
			m.Logger.Infof("leadership state change: %v", ls.State)

			m.mu.Lock()

			// The state the node is transitioning to.
			switch ls.State {
			case keys.Leader, keys.LeaderRX:
				m.isLeader = true
				m.stopStreamReplication()
				m.stopPeer()

			case keys.Follower, keys.Impaired:
				m.isLeader = false
				//m.startLocal(m.ctx)
				m.startPeer(m.ctx)

				done := make(chan error)
				go m.startStreamReplication(ctx, done)
				select {
				case err := <-done:
					if err != nil {
						m.Logger.Warnf("failed to start stream replication: %s", err)
					}
					// TODO: make configurable?
				case <-time.After(3 * time.Second):
					m.Logger.Warnf("timed out waiting for stream replication to start")
				}
			}

			m.leadershipState = ls.State

			m.mu.Unlock()

		case ts := <-tch:
			if !ts.State {
				break
			}

			m.Logger.Infof("transitioning state change: %v", ts.State)

			m.tne.DoneTransitioning(ts.Leadership)
		}
	}
}

func (m *Manager) Init(ctx context.Context) error {
	cctx, cancel := context.WithCancel(ctx)
	m.ctx = cctx
	m.cancel = cancel

	tne, err := tne.New(m.TNEConfig)
	if err != nil {
		return fmt.Errorf("failed to initialze tne: %w", err)
	}

	tne.NatsURL = m.LocalURL
	if err := tne.Start(cctx); err != nil {
		return fmt.Errorf("failed to start tne: %w", err)
	}

	m.tne = tne
	m.Logger.Infof("tne started")

	// Check if we are the configured leader.
	isConfiguredLeader, err := tne.IsConfiguredLeader()
	if err != nil {
		return fmt.Errorf("failed to get leader status: %w", err)
	}
	m.Logger.Infof("is configured leader: %v", isConfiguredLeader)

	// If this node is the configured leader, attempt to connect to the peer
	// to determine if the peer is the leader. If the peer is not the leader,
	// assume leadership.
	if isConfiguredLeader {
		var pnc *nats.Conn
		isFollower := false

		for i := 0; i < 3; i++ {
			pnc, err = nats.Connect(m.PeerURL, m.PeerOpts...)
			if err != nil {
				m.Logger.Warnf("init: failed to connect to peer: attempt %d: %s", i+1, err)
				jitterSleep(time.Second)
				continue
			}

			// Attempt to get the remote state to decide if we should assume leadership.
			peerState, err := tne.GetRemoteLeadershipState(pnc)
			if err == nil {
				m.Logger.Infof("init: peer leadership state: %v", peerState.State)
			} else {
				m.Logger.Warnf("init: failed to get peer leadership state: %s", err)
				continue
			}

			// If the peer is a leader or leaderRX, continue.
			if peerState.State == keys.Leader || peerState.State == keys.LeaderRX {
				m.Logger.Infof("init: peer is leader")
				isFollower = true
				break
			}

			break
		}
		if pnc != nil {
			pnc.Close()
		}

		// If not explicitly indicated as a follower, assume leadership.
		if !isFollower {
			m.isLeader = true
			m.Logger.Infof("init: assuming leadership, configured leader without peer leader state")
		}
	}

	// Initialize local. If this fails, we can't continue.
	if err := m.startLocal(cctx); err != nil {
		cancel()
		return err
	}
	m.Logger.Infof("local initialized")

	if m.isLeader {
		m.Logger.Infof("init: starting as leader")
	} else {
		m.Logger.Infof("init: starting as follower")
	}

	errch := make(chan error, 1)

	// Setup tne watchers.
	lch := make(chan keys.LeadershipKVState, 1)
	go func() {
		if err := m.tne.WatchLeadership(ctx, lch); err != nil {
			errch <- fmt.Errorf("failed to watch leadership: %w", err)
		}
	}()

	tch := make(chan keys.TransitioningKVState, 1)
	go func() {
		if err := m.tne.WatchTransitioning(ctx, tch); err != nil {
			errch <- fmt.Errorf("failed to watch transitioning: %w", err)
		}
	}()

	go m.handleStateChanges(ctx, lch, tch)

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt)
	go func() {
		select {
		case <-sigch:
			m.Logger.Info("received interrupt, stopping")
		case err := <-m.tne.Err():
			m.Logger.Errorf("tne error: %s", err)
		case err := <-errch:
			m.Logger.Errorf("tne watch error: %s", err)
		}
		m.Stop()
	}()

	return nil
}

func (m *Manager) Stop() {
	ctx := m.ctx
	m.cancel()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopStreamReplication()
	m.stopPeer()
	m.stopLocal()
	m.tne.Stop(ctx)
}

func (m *Manager) KeyValue(write bool) *KeyValue {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var kv *KeyValue

	// Leader always points to local.
	// Writes point to leader otherwise local replica for reads.
	if m.isLeader || !write {
		kv = m.lkkv
	} else {
		kv = m.pkkv
	}

	if kv == nil {
		panic(fmt.Sprintf("key-value is nil: isLeader=%v, write=%v", m.isLeader, write))
	}

	return kv
}
