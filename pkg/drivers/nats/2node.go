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

	// Local and peer connections.
	lnc *nats.Conn
	pnc *nats.Conn

	ljs jetstream.JetStream
	pjs jetstream.JetStream

	// Local and peer key-value stores.
	lkv jetstream.KeyValue
	pkv jetstream.KeyValue

	// If true, indicates the local node is the leader.
	isLeader        bool
	leadershipState keys.LeadershipState

	lkkv   *KeyValue
	pkkv   *KeyValue
	ctx    context.Context
	cancel context.CancelFunc

	// Consumer context to the peer stream when replicating.
	cctx jetstream.ConsumeContext

	mu sync.RWMutex

	tne *tne.TNE
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
	// If an explicit sequence number is provided, re-create the bucket.
	// Otherwise, check if the bucket exists.
	if del {
		// Cancel the previous KV instance to stop the watchers.
		if m.lkkv != nil {
			m.lkkv.Stop()
		}

		m.Logger.Infof("init-local: re-creating bucket %s with sequence %d", m.KVConfig.Bucket, seq)
		err := m.ljs.DeleteKeyValue(ctx, m.KVConfig.Bucket)
		if err != nil {
			m.Logger.Errorf("init-local: failed to delete bucket %s: %s", m.KVConfig.Bucket, err)
		}
	} else {
		m.Logger.Infof("into-local: checking if bucket %s exists", m.KVConfig.Bucket)
		_, err := m.ljs.KeyValue(ctx, m.KVConfig.Bucket)
		if err == nil {
			m.lkv, _ = m.ljs.KeyValue(ctx, m.KVConfig.Bucket)
			return m.ljs.Stream(ctx, fmt.Sprintf("KV_%s", m.KVConfig.Bucket))
		}
		m.Logger.Infof("info-local: creating bucket %s", m.KVConfig.Bucket)
	}

	// TODO: update to nats.go 1.33.1 which has the "first revision" option.
	scfg := jetstream.StreamConfig{
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

	for {
		str, err := m.ljs.CreateStream(ctx, scfg)
		if err == nil {
			m.lkv, err = m.ljs.KeyValue(ctx, m.KVConfig.Bucket)
			if err != nil {
				return nil, fmt.Errorf("init-local: failed to get KV: %w", err)
			}

			m.lkkv = NewKeyValue(ctx, m.lkv, m.ljs)
			return str, nil
		}

		// If JetStream is not yet available, retry.
		if errors.Is(err, context.DeadlineExceeded) {
			m.Logger.Warnf("init-local: timed out waiting for bucket %s to be created. retrying", m.KVConfig.Bucket)
			continue
		}

		if err != nil {
			return nil, fmt.Errorf("init-local: failed to initialize KV bucket: %w", err)
		}
	}
}

// initLocal initializes the local connection and ensures the key-value bucket exists.
func (m *Manager) initLocal(ctx context.Context) error {
	opts := append([]nats.Option{}, m.LocalOpts...)
	opts = append(opts,
		nats.MaxReconnects(-1),
		nats.Name("kine-local"),
	)
	// Local connection should always succeed.
	nc, err := nats.Connect(m.LocalURL, opts...)
	if err != nil {
		return fmt.Errorf("init-local: failed to create local connection: %w", err)
	}
	m.lnc = nc

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("init-local: failed to get JetStream context: %w", err)
	}
	m.ljs = js

	// Create the bucket if it doesn't exist. Note, this is a no-op if the bucket
	// already exists with the same configuration.
	if _, err := m.initLocalBucket(ctx, 0, false); err != nil {
		return err
	}
	m.Logger.Infof("init-local: bucket initialized: %s", m.KVConfig.Bucket)

	return nil
}

// initPeer initializes the peer connection and ensures the key-value bucket exists.
func (m *Manager) initPeer(ctx context.Context) {
	// Get a local copy to check.
	m.mu.RLock()
	pnc := m.pnc
	m.mu.RUnlock()

	opts := append([]nats.Option{}, m.PeerOpts...)
	opts = append(opts,
		nats.MaxReconnects(-1),
		nats.Name("kine-peer"),
	)

	var err error
	if pnc == nil {
		i := 0
		for {
			pnc, err = nats.Connect(m.PeerURL, opts...)
			if err != nil {
				i++
				m.Logger.Warnf("init-peer: failed to connect: attempt %d: %s", i, err)
				jitterSleep(time.Second)
				continue
			}
			break
		}
	}

	pjs, _ := jetstream.New(pnc)

	var pkv jetstream.KeyValue
	i := 0
	for {
		pkv, err = pjs.KeyValue(ctx, m.KVConfig.Bucket)
		if err != nil {
			i++
			m.Logger.Warnf("init-peer: failed to get KV: attempt %d: %s", i, err)
			jitterSleep(time.Second)
			continue
		}
		break
	}

	m.mu.Lock()
	m.pnc = pnc
	m.pjs = pjs
	m.pkkv = NewKeyValue(ctx, pkv, pjs)
	m.mu.Unlock()

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
		m.cctx.Stop()
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
	var t0 time.Time

	first := true
	// Keep track of the last sequence number that was published.
	seq := uint64(0)
	str, err := m.ljs.Stream(ctx, sname)
	if err != nil {
		done <- fmt.Errorf("failed to get local stream: %w", err)
		return
	}

	msgHandler := func(msg jetstream.Msg) {
		md, _ := msg.Metadata()

		if first {
			first = false
			t0 = time.Now()
			// Initialize a bucket with one less than the peer's starting sequence.
			// This ensures when we publish the first message, the sequences match.
			str, err = m.initLocalBucket(ctx, md.Sequence.Stream-1, true)
			if err != nil {
				done <- fmt.Errorf("failed to initialize local bucket: %w", err)
				return
			}

			// Set the initial expected sequence.
			seq = md.Sequence.Stream - 1
		}

		// Compare the last sequence we published to the replicated message.
		// If they don't match, this indicates a gap due to interior deletes
		// or expired messages. We need to publish tombstones for the missing
		// sequence numbers.
		// For example, if the local sequence is 10 and the replicated message
		// sequence is 15, then we need to publish tombstones for 11-14.
		if md.Sequence.Stream != seq+1 {
			m.Logger.Debugf("gap detected: %d -> %d", seq, md.Sequence.Stream)
			num := int(md.Sequence.Stream-seq) - 1

			for i := 0; i < num; i++ {
				subject := fmt.Sprintf("$KV.%s.__tomb", m.KVConfig.Bucket)
				pa, err := m.ljs.Publish(ctx, subject, nil)
				if err != nil {
					m.Logger.Debugf("failed to publish tombstone: %s", err)
					continue
				}
				err = str.DeleteMsg(ctx, pa.Sequence)
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
			m.Logger.Warnf("failed to publish local message: %s", err)
		}

		seq = pa.Sequence
		if pa.Sequence != md.Sequence.Stream {
			m.Logger.Debugf("sequence mismatch: %d -> %d", md.Sequence.Stream, pa.Sequence)
		}

		numRemaining--
		if numRemaining == 0 {
			m.Logger.Debugf("caught up with peer")
			m.Logger.Infof("stream caught-up: %d messages in %s", numPending, time.Since(t0))
			done <- nil
		}
	}

	cctx, err := con.Consume(msgHandler, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		// TODO: handle error?
		m.Logger.Warnf("replication consumer error: %s", err)
	}))
	if err != nil {
		done <- fmt.Errorf("failed to start replication consumer: %w", err)
		return
	}

	m.cctx = cctx
	m.Logger.Infof("started stream replication")
}

func (m *Manager) Init(ctx context.Context) error {
	cctx, cancel := context.WithCancel(ctx)
	m.ctx = cctx
	m.cancel = cancel

	tne, err := tne.New(m.TNEConfig)
	if err != nil {
		return fmt.Errorf("failed to initialze TNE: %w", err)
	}

	tne.NatsURL = m.LocalURL
	if err := tne.Start(cctx); err != nil {
		return fmt.Errorf("failed to start TNE: %w", err)
	}

	m.tne = tne
	m.Logger.Infof("TNE started")

	// Initialize local. If this fails, we can't continue.
	if err := m.initLocal(cctx); err != nil {
		cancel()
		return err
	}
	m.Logger.Infof("local initialized")

	errch := make(chan error, 1)

	// Setup TNE watchers.
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

	// Check if we are the configured leader.
	isConfiguredLeader, err := tne.IsConfiguredLeader()
	if err != nil {
		return fmt.Errorf("failed to get leader status: %w", err)
	}
	m.Logger.Infof("is configured leader: %v", isConfiguredLeader)

	// Best effort to initialize a temporary peer connection
	// to check the leadership state of the peer.
	attempts := 0
	for attempts < 3 {
		attempts++
		pnc, err := nats.Connect(m.PeerURL, m.PeerOpts...)
		if err != nil {
			m.Logger.Warnf("init-peer: failed to connect: attempt %d: %s", attempts, err)
			jitterSleep(time.Second)
			continue
		}
		m.pnc = pnc
		break
	}

	for {
		// Attempt to get the remote state to decide if we should assume leadership.
		var peerState keys.LeadershipKVState
		if m.pnc != nil {
			peerState, err = tne.GetRemoteLeadershipState(m.pnc)
			if err == nil {
				m.Logger.Infof("init: peer leadership state: %v", peerState.State)
			} else {
				m.Logger.Warnf("init: failed to get peer leadership state: %s", err)
			}
		} else {
			m.Logger.Warnf("init: failed to connect to peer")
		}

		// If the peer is a leader or leaderRX, continue.
		if peerState.State == keys.Leader || peerState.State == keys.LeaderRX {
			m.Logger.Infof("init: peer is leader")
			break
		}

		// Peer is not the leader, so assume leadership if we are the configured leader.
		if isConfiguredLeader {
			m.isLeader = true
			m.Logger.Infof("init: assuming leadership, configured leader without peer leader state")
			break
		}

		m.Logger.Debugf("init: configured leader is still initializing, waiting...")
		jitterSleep(time.Second)
	}

	go func() {
		for {
			select {
			case ls := <-lch:
				m.Logger.Infof("leadership state change: %v", ls.State)

				m.mu.Lock()
				t0 := time.Now()

				m.leadershipState = ls.State

				switch ls.State {
				case keys.Leader, keys.LeaderRX:
					if !m.isLeader {
						m.isLeader = true
						m.stopStreamReplication()
					}

				case keys.Follower, keys.Impaired:
					if m.isLeader {
						m.isLeader = false
						done := make(chan error)
						go m.startStreamReplication(ctx, done)
						select {
						case err := <-done:
							if err != nil {
								m.Logger.Warnf("failed to start stream replication: %s", err)
							}
						case <-time.After(3 * time.Second):
							m.Logger.Warnf("timed out waiting for stream replication to start")
						}
					}
				}

				m.Logger.Infof("leadership state transitioned in %s", time.Since(t0))

				m.mu.Unlock()

			case ts := <-tch:
				m.Logger.Infof("received transitioning event: %v", ts)
				if !ts.State {
					break
				}

				m.Logger.Infof("transitioning leadership state to: %v", ts.Leadership)

				switch ts.Leadership {
				case keys.Leader, keys.LeaderRX:
				case keys.Follower, keys.Impaired:
				}

				m.tne.DoneTransitioning(ts.Leadership)
			}
		}
	}()

	// Attempt to initialize the peer in the background if we are the leader.
	// Otherwise, initialize the peer and start stream replication.
	if m.isLeader {
		m.Logger.Infof("init: starting as leader")
		go m.initPeer(cctx)
		m.Logger.Infof("init-peer: initialization started in background")
	} else {
		m.Logger.Infof("init: starting as follower")
		m.initPeer(cctx)
		m.Logger.Infof("init-peer: initialization started")

		// Used to signal when the catchup is complete.
		done := make(chan error)
		go m.startStreamReplication(ctx, done)
		select {
		case err := <-done:
			if err != nil {
				m.Logger.Warnf("init: failed to start stream replication: %s", err)
			}
		case <-time.After(3 * time.Second):
			m.Logger.Warnf("init: timed out waiting for stream replication to start")
		}
	}

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt)
	go func() {
		select {
		case <-sigch:
			m.Logger.Info("received interrupt, stopping")
		case err := <-m.tne.Err():
			m.Logger.Errorf("TNE error: %s", err)
		case err := <-errch:
			m.Logger.Errorf("TNE watch error: %s", err)
		}
		m.Stop()
	}()

	return nil
}

func (m *Manager) Stop() {
	m.cancel()
	m.tne.Stop(context.Background())
	if m.lnc != nil {
		m.lnc.Drain()
	}
	m.mu.RLock()
	if m.pnc != nil {
		m.pnc.Drain()
	}
	m.mu.RUnlock()
}

func (m *Manager) KeyValue(write bool) *KeyValue {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Leader always points to local.
	if m.isLeader {
		return m.lkkv
	}

	// Writes point to leader otherwise local replica for reads.
	if write {
		return m.pkkv
	}
	return m.lkkv
}
