package snapshot

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"aequitas-ledger/internal/core"
	"aequitas-ledger/internal/wal"
)

type SnapshotLoop interface {
	RequestSnapshotView() ([]core.Account, int64)
}

type CheckpointerConfig struct {
	Dir              string
	Interval         time.Duration
	MaxSnapshotsKept int
}

func DefaultConfig(dir string) CheckpointerConfig {
	return CheckpointerConfig{
		Dir:              dir,
		Interval:         60 * time.Second,
		MaxSnapshotsKept: 3,
	}
}

type Checkpointer struct {
	cfg  CheckpointerConfig
	loop SnapshotLoop
	wal  *wal.WAL

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewCheckpointer(cfg CheckpointerConfig, loop SnapshotLoop, w *wal.WAL) *Checkpointer {
	if cfg.Interval <= 0 {
		cfg.Interval = 60 * time.Second
	}
	if cfg.MaxSnapshotsKept <= 0 {
		cfg.MaxSnapshotsKept = 3
	}
	c := &Checkpointer{
		cfg:  cfg,
		loop: loop,
		wal:  w,
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

func (c *Checkpointer) Start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.cfg.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-ticker.C:
				if err := c.TakeSnapshot(); err != nil {
					slog.Error("failed to take snapshot", "error", err)
				}
			}
		}
	}()
}

func (c *Checkpointer) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}

func (c *Checkpointer) TakeSnapshot() error {
	if c.loop == nil {
		return nil
	}

	accounts, lsn := c.loop.RequestSnapshotView()
	if len(accounts) == 0 && lsn <= 0 {
		return nil
	}

	snapName := fmt.Sprintf("snapshot-%020d.snap", lsn)
	snapPath := filepath.Join(c.cfg.Dir, snapName)

	if err := Write(snapPath, lsn, accounts); err != nil {
		return fmt.Errorf("failed to write snapshot file: %w", err)
	}

	if c.wal != nil {
		if err := c.wal.AppendCheckpoint(lsn); err != nil {
			slog.Warn("failed to append checkpoint record to WAL", "error", err)
		}
		if err := c.wal.TruncateBefore(lsn); err != nil {
			slog.Warn("failed to truncate WAL segments before LSN", "lsn", lsn, "error", err)
		}
	}

	if err := CleanupOldSnapshots(c.cfg.Dir, c.cfg.MaxSnapshotsKept); err != nil {
		slog.Warn("failed to cleanup old snapshots", "error", err)
	}

	slog.Info("Snapshot completed successfully", "lsn", lsn, "accounts", len(accounts), "file", snapName)
	return nil
}
