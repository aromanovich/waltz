package memcold

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"

	"github.com/aromanovich/waltz/cold"
	"github.com/aromanovich/waltz/wal"
)

// The watermark is this store's bookkeeping, not Temporal's, so it has its own
// table rather than a column in one the server also writes.
const (
	createWatermarksQry = `CREATE TABLE waltz_watermarks (
 shard_id INTEGER NOT NULL,
 seqno INTEGER NOT NULL,
 PRIMARY KEY (shard_id))`

	setWatermarkQry = `INSERT INTO waltz_watermarks (shard_id, seqno) VALUES (?, ?)
 ON CONFLICT (shard_id) DO UPDATE SET seqno = excluded.seqno`

	getWatermarkQry = `SELECT seqno FROM waltz_watermarks WHERE shard_id = ?`
)

var _ cold.Watermarker = (*Store)(nil)

// SetWatermark moves shard's watermark to seqno inside tx, so it commits with
// the rows it vouches for. A watermark in its own transaction could outlive a
// rolled-back batch, or be lost by one that landed.
//
// Last write wins, not the highest: an applier that moves it backwards makes
// the shard re-fold what it already applied. Ordering is the applier's job.
func SetWatermark(ctx context.Context, tx sqlplugin.Tx, shard wal.ShardID, seqno wal.Seqno) error {
	conn, err := txConn(tx)
	if err != nil {
		return err
	}
	// Passed unsigned, so a seqno beyond int64 is a driver error, not a
	// wrapped negative number.
	if _, err := conn.ExecContext(ctx, setWatermarkQry, int64(shard), uint64(seqno)); err != nil {
		return fmt.Errorf("memcold: moving watermark of shard %d to %d: %w", shard, seqno, err)
	}
	return nil
}

// Watermark is the last seqno a drain committed for shard, false if none has.
// A missing row is reported as absence, not as seqno zero.
//
// It opens its own transaction, so it must not be called while holding one on
// this store: with a single connection, that deadlocks.
func (s *Store) Watermark(ctx context.Context, shard wal.ShardID) (wal.Seqno, bool, error) {
	tx, err := s.db.BeginTx(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("memcold: reading watermark of shard %d: %w", shard, err)
	}
	defer func() { _ = tx.Rollback() }()

	conn, err := txConn(tx)
	if err != nil {
		return 0, false, err
	}
	var seqno uint64
	switch err := conn.GetContext(ctx, &seqno, getWatermarkQry, int64(shard)); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("memcold: reading watermark of shard %d: %w", shard, err)
	}
	return wal.Seqno(seqno), true, nil
}

// setupWatermarks adds the table to a database the plugin has just created.
func setupWatermarks(db sqlplugin.DB) error {
	ctx := context.Background()
	tx, err := db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("memcold: creating the watermark table: %w", err)
	}
	conn, err := txConn(tx)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if _, err := conn.ExecContext(ctx, createWatermarksQry); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("memcold: creating the watermark table: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("memcold: creating the watermark table: %w", err)
	}
	return nil
}

// txConn returns the connection a transaction runs on, the only way to reach a
// table Temporal has no interface for. sqlplugin.Tx names only Temporal's
// tables, and its AdminDB Exec runs on the pool, outside the transaction, and
// would deadlock waiting for the connection the transaction holds.
//
// It reads an unexported field, so an upstream rename breaks it. [New] creates
// the table through this path, so that breakage fails construction instead of
// silently losing the watermark.
func txConn(tx sqlplugin.Tx) (sqlplugin.Conn, error) {
	v := reflect.ValueOf(tx)
	if v.Kind() == reflect.Pointer && !v.IsNil() {
		if f := v.Elem().FieldByName("conn"); f.IsValid() && f.CanAddr() {
			if conn, ok := reflect.NewAt(f.Type(), f.Addr().UnsafePointer()).Elem().Interface().(sqlplugin.Conn); ok {
				return conn, nil
			}
		}
	}
	return nil, fmt.Errorf("memcold: %T exposes no connection to run the watermark table's own SQL on", tx)
}
