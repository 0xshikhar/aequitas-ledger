package replication

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"aequitas-ledger/internal/observability"
	"aequitas-ledger/internal/wal"
)

// replicationPollInterval bounds how long a follower can sit unaware of new
// primary commits while the stream is caught up.
const replicationPollInterval = 5 * time.Millisecond

// Server streams the primary's committed WAL records to followers (C0.8):
//   - handshake: the follower sends its resume LSN (8B); the server replies
//     with its leader term (8B);
//   - every frame is [term:8][LSN:8][type:1][len:4][payload][crc32:4] — the
//     term prefix lets followers fence stale primaries;
//   - the connection stays open and tails the WAL via StreamReader (no full
//     re-scan per reconnect), polling at replicationPollInterval when caught
//     up;
//   - HEAD frames (stream-only) announce the durable head LSN so followers
//     can report true lag.
type Server struct {
	addr      string
	wal       *wal.WAL
	term      uint64
	listener  net.Listener
	tlsConfig *tls.Config
	conns     atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewServer(addr string, w *wal.WAL) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		addr:   addr,
		wal:    w,
		ctx:    ctx,
		cancel: cancel,
	}
}

// SetTLSConfig configures TLS/mTLS encryption for incoming follower connections (P5.3).
func (s *Server) SetTLSConfig(cfg *tls.Config) {
	s.tlsConfig = cfg
}

// Addr returns the network address the server is listening on.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

func (s *Server) Start() error {
	term, err := PrimaryTerm(s.wal)
	if err != nil {
		return err
	}
	s.term = term

	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("replication server listen failed: %w", err)
	}

	if s.tlsConfig != nil {
		lis = tls.NewListener(lis, s.tlsConfig)
	}
	s.listener = lis

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.ctx.Done():
					return
				default:
					continue
				}
			}
			s.wg.Add(1)
			go func(c net.Conn) {
				defer s.wg.Done()
				defer c.Close()
				s.handleConn(c)
			}(conn)
		}
	}()
	return nil
}

func (s *Server) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
}

func (s *Server) handleConn(conn net.Conn) {
	observability.ReplicationPrimaryFollowers.Set(float64(s.conns.Add(1)))
	defer func() { observability.ReplicationPrimaryFollowers.Set(float64(s.conns.Add(-1))) }()

	// Handshake: resume LSN in, leader term out.
	var reqLSNBuf [8]byte
	if _, err := io.ReadFull(conn, reqLSNBuf[:]); err != nil {
		return
	}
	startLSN := int64(binary.BigEndian.Uint64(reqLSNBuf[:]))
	var termBuf [8]byte
	binary.BigEndian.PutUint64(termBuf[:], s.term)
	if _, err := conn.Write(termBuf[:]); err != nil {
		return
	}

	sr := s.wal.NewStreamReader(startLSN)
	defer sr.Close()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		rec, ok, err := sr.Next()
		if err != nil {
			return // corrupted durable WAL: fail the stream loudly
		}
		if !ok {
			// Caught up: announce the durable head (so the follower can
			// compute lag), then poll for new commits.
			head := s.wal.CurrentLSN()
			if err := writeFrame(conn, s.term, wal.Record{
				Type:    wal.RecordTypeHead,
				Payload: beUint64(uint64(head)),
			}); err != nil {
				return
			}
			sleepCtx(s.ctx, replicationPollInterval)
			continue
		}
		if err := writeFrame(conn, s.term, rec); err != nil {
			return
		}
	}
}

func beUint64(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}

// writeFrame emits [term][LSN][type][len][payload][crc32] — the CRC covers
// the term so term tampering is detected as corruption.
func writeFrame(conn net.Conn, term uint64, rec wal.Record) error {
	payloadLen := len(rec.Payload)
	frameLen := 8 + 8 + 1 + 4 + payloadLen + 4
	buf := make([]byte, frameLen)
	binary.BigEndian.PutUint64(buf[0:8], term)
	binary.BigEndian.PutUint64(buf[8:16], rec.LSN)
	buf[16] = byte(rec.Type)
	binary.BigEndian.PutUint32(buf[17:21], uint32(payloadLen))
	copy(buf[21:21+payloadLen], rec.Payload)
	checksum := crc32.ChecksumIEEE(buf[:21+payloadLen])
	binary.BigEndian.PutUint32(buf[21+payloadLen:], checksum)
	_, err := conn.Write(buf)
	return err
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
