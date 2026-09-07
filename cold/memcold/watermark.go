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

// The watermark is this database's bookkeeping and not Temporal's, so it gets a
// table of its own beside the schema the plugin sets up rather than a column
// borrowed from a table the server also writes.
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

// SetWatermark moves shard's watermark to seqno in tx, and is how an applier
// keeps the promise the cold package's doc makes: the watermark commits in the
// transaction that carries the rows it vouches for. It takes the transaction
// rather than opening one because that is the whole of the guarantee — a
// watermark with a transaction of its own can outlive a rolled-back batch, or
// be lost by a batch that landed.
//
// Last write wins. The row is what the last committed drain put there and not
// the highest any drain ever wrote, so an applier that moves it backwards gets a
// shard that re-folds what it already applied; ordering is the applier's.
func SetWatermark(ctx context.Context, tx sqlplugin.Tx, shard wal.ShardID, seqno wal.Seqno) error {
	conn, err := txConn(tx)
	if err != nil {
		return err
	}
	// Unsigned across the driver boundary: a seqno past what a signed 64-bit
	// column holds comes back as an error instead of a wrapped number.
	if _, err := conn.ExecContext(ctx, setWatermarkQry, int64(shard), uint64(seqno)); err != nil {
		return fmt.Errorf("memcold: moving watermark of shard %d to %d: %w", shard, seqno, err)
	}
	return nil
}

// Watermark is the last seqno a drain committed for shard, false if none has.
// The two answers differ: a shard that has never drained is not a shard drained
// up to zero, and a replay told the second would start above entries it has to
// fold.
//
// It reads in a transaction of its own, so a caller already holding one on this
// store must not call it — the database is served by a single connection and the
// read would wait on the transaction that is waiting for it.
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

// txConn is the connection a transaction runs on, and the only way to reach a
// table Temporal has no interface for. sqlplugin.Tx is TableCRUD, which names
// Temporal's own tables and nothing else; the Exec that the same value carries
// as an AdminDB runs on the connection pool rather than on the transaction, so
// it would leave the transaction it was meant to join and then wait for the
// connection that transaction is holding.
//
// The field is unexported, so an upstream that renames it breaks this. [New]
// creates the table through this same path, which is what turns that into a
// store that refuses to be built rather than a drain that loses its watermark.
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
