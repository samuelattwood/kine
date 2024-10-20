package nats

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sirupsen/logrus"
	"github.com/synadia-labs/tne/keys"
)

var (
	baseSubnet = "192.168.68.0"   //os.Getenv("BASE_SUBNET")
	baseIP     = "192.168.68.116" //os.Getenv("BASE_IP")
)

type tneConfig struct {
	Leader     bool
	NatsURL    string
	BaseSubnet string
	BaseIP     string
	RemotePort int
	FsmzPort   int
	LivezPort  int
}

func (c *tneConfig) File(t *testing.T) string {
	config := fmt.Sprintf(`leader = %v
clean = true
apistart = true
externaltransitioning = true
natsurl = "%s"
floatingip = "%s/24"
remotefsmz = "http://%s:%d"
port = "%d"
localapi = "http://%s:%d/livez"
`, c.Leader, c.NatsURL, c.BaseSubnet, c.BaseIP, c.RemotePort, c.FsmzPort, c.BaseIP, c.LivezPort)

	d := t.TempDir()
	path := fmt.Sprintf("%s/tne.conf", d)
	err := os.WriteFile(path, []byte(config), 0644)
	noErr(t, err)
	return path
}

func newLivezServer(t *testing.T) (*http.Server, net.Listener) {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	ls, err := net.Listen("tcp", fmt.Sprintf("%s:0", baseIP))
	noErr(t, err)

	s := &http.Server{
		Handler: mux,
	}
	go func() {
		err := s.Serve(ls)
		noErr(t, err)
	}()
	return s, ls
}

func Test2Node_Basic(t *testing.T) {
	ctx := context.Background()

	// Start NATS servers.
	ns1 := test.RunServer(&server.Options{
		Host:      baseIP,
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	t.Logf("ns1: %s", ns1.ClientURL())
	defer ns1.Shutdown()

	ns2 := test.RunServer(&server.Options{
		Host:      baseIP,
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	t.Logf("ns2: %s", ns2.ClientURL())
	defer ns2.Shutdown()

	// Start mock k3s livez servers.
	ls1, p1 := newLivezServer(t)
	defer ls1.Shutdown(ctx)
	t.Logf("livez1: %s", p1.Addr())

	ls2, p2 := newLivezServer(t)
	defer ls2.Shutdown(ctx)
	t.Logf("livez2: %s", p2.Addr())

	// Create TNE configs.
	tc1 := tneConfig{
		Leader:     true,
		NatsURL:    ns1.ClientURL(),
		BaseSubnet: baseSubnet,
		BaseIP:     baseIP,
		FsmzPort:   51217,
		RemotePort: 51218,
		LivezPort:  p1.Addr().(*net.TCPAddr).Port,
	}
	tn1 := tc1.File(t)
	defer os.Remove(tn1)

	tc2 := tneConfig{
		Leader:     false,
		NatsURL:    ns2.ClientURL(),
		BaseSubnet: baseSubnet,
		BaseIP:     baseIP,
		FsmzPort:   51218,
		RemotePort: 51217,
		LivezPort:  p2.Addr().(*net.TCPAddr).Port,
	}
	tn2 := tc2.File(t)
	defer os.Remove(tn2)

	// Create managers.
	m1 := Manager{
		LocalURL:  ns1.ClientURL(),
		PeerURL:   ns2.ClientURL(),
		TNEConfig: tn1,
		KVConfig: &jetstream.KeyValueConfig{
			Bucket:   "test",
			History:  10,
			Replicas: 1,
		},
		Logger: logrus.New(),
	}

	m2 := Manager{
		LocalURL:  ns2.ClientURL(),
		PeerURL:   ns1.ClientURL(),
		TNEConfig: tn2,
		KVConfig: &jetstream.KeyValueConfig{
			Bucket:   "test",
			History:  10,
			Replicas: 1,
		},
		Logger: logrus.New(),
	}

	// Wait until they are ready...
	ready := make(chan struct{}, 2)
	go func() {
		err := m1.Init(ctx)
		noErr(t, err)
		ready <- struct{}{}
	}()
	defer m1.Stop()

	go func() {
		err := m2.Init(ctx)
		noErr(t, err)
		ready <- struct{}{}
	}()
	defer m2.Stop()

	<-ready
	<-ready
	close(ready)

	// Assert states.
	expEqual(t, m1.GetState(), keys.Leader)
	expEqual(t, m2.GetState(), keys.Follower)

	// Put a value on leader.
	seq, err := m1.lkkv.Create(ctx, "foo", []byte("bar"))
	noErr(t, err)
	expEqual(t, seq, 1)

	seq, err = m1.lkkv.Update(ctx, "foo", []byte("baz"), seq)
	noErr(t, err)
	expEqual(t, seq, 2)

	// Wait for replication.
	time.Sleep(10 * time.Millisecond)

	entry, err := m2.lkkv.Get(ctx, "foo")
	noErr(t, err)
	expEqual(t, string(entry.Value()), "baz")
	expEqual(t, entry.Revision(), 2)
}

func newKVStream(t *testing.T, ctx context.Context, js jetstream.JetStream, bucket string) jetstream.Stream {
	cfg := jetstream.StreamConfig{
		Name:              fmt.Sprintf("KV_%s", bucket),
		Subjects:          []string{fmt.Sprintf("$KV.%s.>", bucket)},
		Storage:           jetstream.FileStorage,
		Replicas:          1,
		MaxMsgsPerSubject: int64(10),
		AllowRollup:       true,
		DenyDelete:        false,
		AllowDirect:       true,
		MaxBytes:          -1,
		MaxMsgSize:        -1,
		MaxMsgs:           -1,
		MaxConsumers:      -1,
	}

	str, err := js.CreateStream(ctx, cfg)
	noErr(t, err)

	return str
}

func TestStreamingReplication(t *testing.T) {
	// Start NATS servers.
	ns1 := test.RunServer(&server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	t.Logf("ns1: %s", ns1.ClientURL())
	defer ns1.Shutdown()

	ns2 := test.RunServer(&server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	t.Logf("ns2: %s", ns2.ClientURL())
	defer ns2.Shutdown()

	nc1, err := nats.Connect(ns1.ClientURL())
	noErr(t, err)
	defer nc1.Close()

	nc2, err := nats.Connect(ns2.ClientURL())
	noErr(t, err)
	defer nc2.Close()

	js1, _ := jetstream.New(nc1)
	js2, _ := jetstream.New(nc2)

	ctx := context.Background()

	str1 := newKVStream(t, ctx, js1, "test")
	str2 := newKVStream(t, ctx, js2, "test")

	// Create a KV on the first server.
	kv1, err := js1.KeyValue(ctx, "test")
	noErr(t, err)

	// Add some data to the first server.
	// Old values will be deleted as the history limit is reached.
	rnd := rand.New(rand.NewSource(1234))
	for i := 0; i < 2000; i++ {
		i := rnd.Intn(50)
		_, err := kv1.Put(ctx, fmt.Sprintf("%d", i), nil)
		noErr(t, err)
	}

	con, err := js1.OrderedConsumer(ctx, "KV_test", jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	noErr(t, err)

	// Indicates when we have caught up with the peer.
	numPending := con.CachedInfo().NumPending
	numRemaining := numPending
	t0 := time.Now()

	// Keep track of the last sequence number that was published.
	seq := uint64(0)
	done := make(chan error, 1)

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
				noErr(t, fmt.Errorf("sequence regression: %d -> %d", seq, md.Sequence.Stream))
			}

			num := int(md.Sequence.Stream-seq) - 1
			t.Logf("gap detected: %d -> %d", seq, md.Sequence.Stream)

			for i := 0; i < num; i++ {
				subject := fmt.Sprintf("$KV.%s.__tomb", "test")
				pa, err := js2.Publish(ctx, subject, nil)
				if err != nil {
					noErr(t, err)
				}
				err = str2.DeleteMsg(ctx, pa.Sequence)
				if err != nil {
					noErr(t, err)
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

		pa, err := js2.PublishMsg(ctx, nmsg)
		if err != nil {
			noErr(t, err)
		}

		seq = pa.Sequence
		if pa.Sequence != md.Sequence.Stream {
			noErr(t, fmt.Errorf("sequence mismatch: %d -> %d", md.Sequence.Stream, pa.Sequence))
		}

		numRemaining--
		if numRemaining == 0 {
			t.Logf("stream caught-up: %d messages in %s", numPending, time.Since(t0))
			done <- nil
		}
	}

	cctx, err := con.Consume(msgHandler, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		t.Logf("replication consumer error: %s", err)
	}))
	noErr(t, err)
	defer cctx.Stop()

	<-done

	i1, err := str1.Info(ctx)
	noErr(t, err)

	i2, err := str2.Info(ctx)
	noErr(t, err)

	t.Logf("str1: first=%d, last=%d, bytes=%d, subs=%d deleted=%d", i1.State.FirstSeq, i1.State.LastSeq, i1.State.Bytes, i1.State.NumSubjects, i1.State.NumDeleted)
	t.Logf("str2: first=%d, last=%d, bytes=%d, subs=%d deleted=%d", i2.State.FirstSeq, i2.State.LastSeq, i2.State.Bytes, i2.State.NumSubjects, i2.State.NumDeleted)

	for i := 0; i < 10000; i++ {
		i := rnd.Intn(50)
		_, err := kv1.Put(ctx, fmt.Sprintf("%d", i), nil)
		noErr(t, err)
	}

	// Wait for replication.
	time.Sleep(200 * time.Millisecond)

	i1, err = str1.Info(ctx)
	noErr(t, err)
	i2, err = str2.Info(ctx)
	noErr(t, err)

	t.Logf("str1: first=%d, last=%d, bytes=%d, subs=%d deleted=%d", i1.State.FirstSeq, i1.State.LastSeq, i1.State.Bytes, i1.State.NumSubjects, i1.State.NumDeleted)
	t.Logf("str2: first=%d, last=%d, bytes=%d, subs=%d deleted=%d", i2.State.FirstSeq, i2.State.LastSeq, i2.State.Bytes, i2.State.NumSubjects, i2.State.NumDeleted)
}
