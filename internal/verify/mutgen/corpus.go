package mutgen

import (
	"fmt"

	"github.com/aromanovich/waltz/mutation"
)

// Stream is a materialised corpus: what [Corpus] generated, what the WAL would
// carry for it, and what came out.
type Stream struct {
	Mutations []mutation.Mutation
	// Payloads are the mutations encoded before any consumer has touched a
	// request, so a second path over one stream reads the log's bytes rather
	// than a rendering of what the first path already stamped.
	Payloads [][]byte
	Report   Report
}

// Corpus generates n mutations from cfg and materialises them: the mutations,
// the bytes a log would carry for each, and the report a corpus is judged on
// ([Report.Missing]). Every error names the seed, since the seed is the whole
// of what reproduces the failure.
//
// A run of 10^5 mutations and up drives [Generator.Next] itself: a materialised
// stream of that size is gigabytes of payload, and a consumer folding as it
// goes needs none of it kept.
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
