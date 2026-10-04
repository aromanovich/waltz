package mutgen

import (
	"fmt"

	"github.com/aromanovich/waltz/mutation"
)

// Stream is a materialised corpus: the mutations [Corpus] generated, their
// encoded payloads, and the generator's report.
type Stream struct {
	Mutations []mutation.Mutation
	// Payloads are encoded before any consumer touches a request, so a second
	// path reads untouched bytes.
	Payloads [][]byte
	Report   Report
}

// Corpus generates n mutations from cfg and keeps them all in memory with
// their payloads and report ([Report.Missing]). Errors name the seed, which
// reproduces them. For 10^5 mutations or more, call [Generator.Next] directly
// instead of holding everything at once.
func Corpus(cfg Config, n int) (Stream, error) {
	g, err := New(cfg)
	if err != nil {
		return Stream{}, err
	}

	out := Stream{
		Mutations: make([]mutation.Mutation, 0, n),
		Payloads:  make([][]byte, 0, n),
	}
	for i := range n {
		m, err := g.Next()
		if err != nil {
			return Stream{}, fmt.Errorf("generating mutation %d of seed %d: %w", i, cfg.Seed, err)
		}
		payload, err := mutation.Encode(m)
		if err != nil {
			return Stream{}, fmt.Errorf("encoding mutation %d of seed %d: %w", i, cfg.Seed, err)
		}
		out.Mutations = append(out.Mutations, m)
		out.Payloads = append(out.Payloads, payload)
	}
	out.Report = g.Report()
	return out, nil
}
