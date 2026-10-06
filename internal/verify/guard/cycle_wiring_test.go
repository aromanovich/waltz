package guard

// cycle.Manager must satisfy each face of wrapper.ShardLayer: ShardObserver
// (an acquire fences the log at the new epoch, then installs the cycle),
// ShardWriter and ShardReader (intercept mode's write and four reads), and
// MetricsSink (the server's metrics handler, handed over after composition).
// ShardLayer already embeds all four; each is also asserted alone so a lost
// method fails under its own name. The two packages never import each other,
// so this composition is the only place a renamed method is caught (waltz.go
// also catches MetricsSink by compiling).

import (
	"github.com/aromanovich/waltz/cycle"
	"github.com/aromanovich/waltz/wrapper"
)

var (
	_ wrapper.ShardObserver = (*cycle.Manager)(nil)
	_ wrapper.ShardWriter   = (*cycle.Manager)(nil)
	_ wrapper.ShardReader   = (*cycle.Manager)(nil)
	_ wrapper.ShardLayer    = (*cycle.Manager)(nil)
	_ wrapper.MetricsSink   = (*cycle.Manager)(nil)
)
