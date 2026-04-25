package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"aequitas-ledger/internal/core"
)

const (
	MagicHeader = "LEDGER01"
	AccountSize = 64
)

var (
	ErrInvalidSnapshotMagic = errors.New("invalid snapshot magic header")
	ErrSnapshotCorrupted    = errors.New("snapshot checksum mismatch (file corrupted)")
	ErrInvalidSnapshotSize  = errors.New("invalid snapshot file size")
)

// Write serializes accounts slice and snapshot LSN into a binary file at path using atomic write-then-rename.
func Write(path string, lsn int64, accounts []core.Account) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	tmpPath := fmt.Sprintf("%s.tmp-%d", path, timeNowNano())
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmpPath)
	}()

	hasher := crc32.NewIEEE()
	writer := io.MultiWriter(f, hasher)

	// 1. Magic (8 bytes)
	if _, err := writer.Write([]byte(MagicHeader)); err != nil {
		return err
	}

	// 2. LSN (8 bytes)
	var b8 [8]byte
	binary.BigEndian.PutUint64(b8[:], uint64(lsn))
	if _, err := writer.Write(b8[:]); err != nil {
		return err
	}

	// 3. Account Count (8 bytes)
	binary.BigEndian.PutUint64(b8[:], uint64(len(accounts)))
	if _, err := writer.Write(b8[:]); err != nil {
		return err
	}

	// 4. Accounts payload (64 bytes per account)
	for i := range accounts {
		raw := core.EncodeAccountPayload(accounts[i])
		if _, err := writer.Write(raw); err != nil {
			return err
		}
	}

	// 5. CRC32 Checksum (4 bytes)
	checksum := hasher.Sum32()
	var b4 [4]byte
	binary.BigEndian.PutUint32(b4[:], checksum)
	if _, err := f.Write(b4[:]); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, path)
}

// Read loads and validates a snapshot file, returning accounts and LSN.
func Read(path string) ([]core.Account, int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}

	if len(data) < 8+8+8+4 { // magic + lsn + count + crc32
		return nil, 0, ErrInvalidSnapshotSize
	}

	// Validate CRC32
	content := data[:len(data)-4]
	expectedCRC := binary.BigEndian.Uint32(data[len(data)-4:])
	actualCRC := crc32.ChecksumIEEE(content)
	if expectedCRC != actualCRC {
		return nil, 0, ErrSnapshotCorrupted
	}

	magic := string(data[:8])
	if magic != MagicHeader {
		return nil, 0, ErrInvalidSnapshotMagic
	}

	lsn := int64(binary.BigEndian.Uint64(data[8:16]))
	count := binary.BigEndian.Uint64(data[16:24])

	expectedPayloadLen := 8 + 8 + 8 + (count * AccountSize)
	if uint64(len(content)) != expectedPayloadLen {
		return nil, 0, ErrInvalidSnapshotSize
	}

	accounts := make([]core.Account, count)
	payload := data[24:len(content)]

	for i := uint64(0); i < count; i++ {
		offset := i * AccountSize
		acc, err := core.DecodeAccountPayload(payload[offset : offset+AccountSize])
		if err != nil {
			return nil, 0, fmt.Errorf("decode snapshot account %d: %w", i, err)
		}
		accounts[i] = acc
	}

	return accounts, lsn, nil
}

// Latest returns the path and LSN of the latest valid .snap file in dir.
func Latest(dir string) (string, int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", 0, nil
		}
		return "", 0, err
	}

	var latestPath string
	var maxLSN int64 = -1

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".snap") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".snap")
		parts := strings.Split(name, "-")
		if len(parts) < 2 {
			continue
		}
		lsn, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
		if err != nil {
			continue
		}
		fullPath := filepath.Join(dir, entry.Name())
		if lsn > maxLSN {
			// Verify file integrity
			_, lsnRead, err := Read(fullPath)
			if err == nil && lsnRead == lsn {
				maxLSN = lsn
				latestPath = fullPath
			}
		}
	}

	if maxLSN == -1 {
		return "", 0, nil
	}
	return latestPath, maxLSN, nil
}

// CleanupOldSnapshots keeps only maxKept most recent .snap files in dir.
func CleanupOldSnapshots(dir string, maxKept int) error {
	if maxKept <= 0 {
		return nil
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	type snapFile struct {
		path string
		lsn  int64
	}

	var snaps []snapFile
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".snap") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".snap")
		parts := strings.Split(name, "-")
		if len(parts) < 2 {
			continue
		}
		lsn, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
		if err != nil {
			continue
		}
		snaps = append(snaps, snapFile{
			path: filepath.Join(dir, entry.Name()),
			lsn:  lsn,
		})
	}

	if len(snaps) <= maxKept {
		return nil
	}

	sort.Slice(snaps, func(i, j int) bool { return snaps[i].lsn > snaps[j].lsn })

	for i := maxKept; i < len(snaps); i++ {
		_ = os.Remove(snaps[i].path)
	}

	return nil
}

func timeNowNano() int64 {
	return time.Now().UnixNano()
}
