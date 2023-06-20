package nats

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
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
