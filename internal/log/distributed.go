package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	api "github.com/denisschmidt/golog/api/v1"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

/*
Set up Raft:
• A finite-state machine that applies the commands you give Raft;

• A log store where Raft stores those commands;

• A stable store where Raft stores the cluster’s configuration—the servers
in the cluster, their addresses, and so on;

• A snapshot store where Raft stores compact snapshots of its data; and

• A transport that Raft uses to connect with the server’s peers
*/

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	d := &DistributedLog{
		config: config,
	}

	if err := d.setupLog(dataDir); err != nil {
		return nil, err
	}

	if err := d.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return d, nil
}

// creates the log for the server where server will store the user's records
func (d *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	d.log, err = NewLog(logDir, d.config)
	return err
}

func (d *DistributedLog) setupRaft(dataDir string) error {
	// finite-state machine
	fsm := &fsm{log: d.log}

	logDir := filepath.Join(dataDir, "raft", "log")

	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	logConfig := d.config
	logConfig.Segment.InitialOffset = 1
	logStore, err := newLogStore(logDir, logConfig)

	if err != nil {
		return err
	}

	// create a new KVS where Raft stores metadata
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))

	if err != nil {
		return err
	}

	retain := 1 // specifies that we keep one snapshot
	/*
		Snapshot stores the state to recover and restore data
		Example:
			If EC2 instance failed and an autoscaling group brought up another instance for the Raft server
			Rather than streaming all the data from the Raft leader, the new server would restore
			from the snapshot (which you could store in S3 or a similar storage service)
			and then get the latest changes from the leader
	*/
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr,
	)

	if err != nil {
		return err
	}

	maxPool := 5
	timeout := 10 * time.Second

	transport := raft.NewNetworkTransport(
		d.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	config := raft.DefaultConfig()
	config.LocalID = d.config.Raft.LocalID // unique Id for the server

	if d.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = d.config.Raft.HeartbeatTimeout
	}

	if d.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = d.config.Raft.ElectionTimeout
	}

	if d.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = d.config.Raft.LeaderLeaseTimeout
	}

	if d.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = d.config.Raft.CommitTimeout
	}

	// create the Raft instance and bootstrap the cluster
	d.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)

	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapshotStore,
	)

	if err != nil {
		return err
	}

	if d.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: raft.ServerAddress(d.config.Raft.BindAddr),
			}},
		}
		err = d.raft.BootstrapCluster(config).Error()
	}

	return err
}

// use public APIs that append records to and read records from the log and wrap Raft
func (d *DistributedLog) Append(record *api.Record) (uint64, error) {
	/*
		Raft apply a command that tell the FSM to append the record to the log
	*/
	res, err := d.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)

	if err != nil {
		return 0, err
	}

	return res.(*api.ProduceResponse).Offset, nil
}

// here we don't use Raft replication (not need strong consistency)
// if reads must be up-to-date with writes, then you must go through Raft,
// but then reads are less efficient and take longer
func (d *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return d.log.Read(offset)
}

// wraps Raft's API to apply requests and return their responses
func (d *DistributedLog) apply(reqType RequestType, req proto.Message) (interface{}, error) {
	var buf bytes.Buffer

	_, err := buf.Write([]byte{byte(reqType)})

	if err != nil {
		return nil, err
	}

	b, err := proto.Marshal(req)

	if err != nil {
		return nil, err
	}

	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}

	timeout := 10 * time.Second
	// replicate the record and append the record to the leader's log
	future := d.raft.Apply(buf.Bytes(), timeout)

	// Raft's replication failed
	if future.Error() != nil {
		return nil, future.Error()
	}

	// check whether the value is an error with a type assertion
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}

	return res, nil
}

/*

Raft defers the running of your business logic to the FSM. FSM must access the data it manages.

In our service, that’s a log, and the FSM appends records to the log.

FSM must implement three methods:
 • Apply(record *raft.Log) — Raft invokes this method after committing a log entry

 • Snapshot() — Raft periodically calls this method to snapshot its state. For
	most services, you’ll be able to build a compacted log for example, if we
	were building a key-value store and we had a bunch of commands saying
	“set foo to bar,” “set foo to baz,” “set foo to qux,” and so on, we would only
	set the latest command to restore the current state.
	Because we’re replicating a log itself, we need the full log to restore it !!!

 • Restore(io.ReadCloser) — Raft calls this to restore an FSM from a snapshot—for
	instance, if an EC2 instance failed and a new instance took its place.

*/

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

func (f *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])

	switch reqType {
	case AppendRequestType:
		return f.applyAppend(buf[1:])
	}

	return nil
}

func (f *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	offset, err := f.log.Append(req.Record)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Offset: offset}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

// represents a point-in-time snapshot of the FSM’s state
var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

func (s *snapshot) Release() {
	//TODO implement me
	panic("implement me")
}

// Store Raft's log data in the local file
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

// Restore an FSM from a snapshot
// FSM must discard existing state to make sure its state will match the leader's replicated state
func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer

	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		size := int64(enc.Uint64(b))
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}

		record := &api.Record{}

		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}

		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}
		// read the records in the snapshot and append them to our new log
		if _, err = f.log.Append(record); err != nil {
			return err
		}

		buf.Reset()
	}

	return nil
}

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

// Wrap our Log implementation for Raft usage
func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	off, err := l.UpperOffset()
	return off, err
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}
	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term
	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}

// removes the records between the offsets—it’s to
// remove records that are old or stored in a snapshot
func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

/*
Raft uses a stream layer in the transport to provide a low-level stream
abstraction to connect with Raft servers. Our stream layer must satisfy Raft’s StreamLayer
*/

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

const RaftRPC = 1

// Dial - makes outgoing connections to other servers in the Raft cluster
// When we connect to a server, we write the
// RaftRPC byte to identify the connection type so we can multiplex Raft on the
// same port as our Log gRPC requests
func (s *StreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}

	var conn, err = dialer.Dial("tcp", string(addr))

	if err != nil {
		return nil, err
	}

	// identify to mux that this is a Raft RPC
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}
	return conn, err
}

func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}
	if bytes.Compare([]byte{byte(RaftRPC)}, b) != 0 {
		return nil, fmt.Errorf("not a raft rpc")
	}
	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}
	return conn, err
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}

// Add server to the Raft cluster
func (l *DistributedLog) Join(id, addr string) error {
	configureFeature := l.raft.GetConfiguration()
	if err := configureFeature.Error(); err != nil {
		return err
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)

	for _, srv := range configureFeature.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				// server has already joined
				return nil
			}
			// remove the existing server
			removeFuture := l.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}

	addFuture := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}

	return nil
}

// Remove the server from the cluster, remove the leader will trigger a new election
func (l *DistributedLog) Leave(id string) error {
	removeFuture := l.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

// block until the cluster has elected a leader or times out
func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if l := l.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

// Shuts down the Raft instance and close the local log
func (l *DistributedLog) Close() error {
	f := l.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return l.log.Close()
}

func (l *DistributedLog) GetServers() ([]*api.Server, error) {
	future := l.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	var servers []*api.Server
	for _, server := range future.Configuration().Servers {
		servers = append(servers, &api.Server{
			Id:       string(server.ID),
			RpcAddr:  string(server.Address),
			IsLeader: l.raft.Leader() == server.Address,
		})
	}
	return servers, nil
}



/*

1 - 0.0137

100000 - 1370

100000 - 1458 - 68.58
100000 - 1370 -


68.58 - 1458
64.00 - 1370

68.58



1370 - 100
1458 - x






 */