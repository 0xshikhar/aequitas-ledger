package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"sync"

	"aequitas-ledger/internal/wal"
)

type Server struct {
	addr     string
	wal      *wal.WAL
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
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

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("replication server listen failed: %w", err)
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
	// Read requested start LSN (8 bytes)
	var reqLSNBuf [8]byte
	if _, err := io.ReadFull(conn, reqLSNBuf[:]); err != nil {
		return
	}
	startLSN := int64(binary.BigEndian.Uint64(reqLSNBuf[:]))

	// Stream WAL records starting from startLSN
	_ = s.wal.RecoverFromLSN(startLSN-1, func(r wal.Record) error {
		if int64(r.LSN) < startLSN {
			return nil
		}

		// Frame: [LSN: 8B] [Type: 1B] [Len: 4B] [Payload: NB] [CRC32: 4B]
		payloadLen := len(r.Payload)
		frameLen := 8 + 1 + 4 + payloadLen + 4
		buf := make([]byte, frameLen)
		binary.BigEndian.PutUint64(buf[0:8], r.LSN)
		buf[8] = byte(r.Type)
		binary.BigEndian.PutUint32(buf[9:13], uint32(payloadLen))
		copy(buf[13:13+payloadLen], r.Payload)
		checksum := crc32.ChecksumIEEE(buf[:13+payloadLen])
		binary.BigEndian.PutUint32(buf[13+payloadLen:], checksum)

		if _, err := conn.Write(buf); err != nil {
			return errors.New("replication client disconnected")
		}
		return nil
	})
}
