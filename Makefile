.DEFAULT_GOAL := test

# Both linters are pinned here and run with `go run` rather than added to
# go.mod: a linter's dependency tree is not a build input for anything this
# module produces, and `tool` directives would put several hundred indirect
# requirements into the file a Temporal bump has to be readable in.
GOLANGCI := github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
MODERNIZE := golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@v0.23.0
GOVULNCHECK := golang.org/x/vuln/cmd/govulncheck@v1.8.0

# No `-p 1` here, and its absence is the deliberate half: nothing in this module
# wants a cluster, a container or a fixed port. Every backend lives in the test
# process and every database is keyed by a name minted per store, so the packages
# share nothing and run concurrently — including internal/verify/e2e, whose Temporal
# services take ports from the OS. `-count=1` is what keeps a green run from
# being yesterday's cache.
.PHONY: test
test: ## Run every test in the module (default target)
	go test ./... -count=1

# The race detector, as its own target rather than a flag on the one above,
# because the two answer different questions and only one of them is cheap. This
# layer is a goroutine per shard owning an accumulator and a drain, two mirrors
# published for readers off that goroutine, a trim beside it and four reads
# served on it — so "does the shipped concurrency hold" is a question `test`
# cannot answer at all, and a green run without this says nothing about it.
#
# The stream is shortened to a tenth: under -race the acceptance is 25× its own
# wall clock and nothing this target looks for needs the extra length, a
# concurrent access being reached by a short stream as well as by a long one.
# What the shortening costs is the volume claim, which `test` already makes.
.PHONY: race
race: ## Run every test under the race detector
	WAL_ACCEPTANCE_MUTATIONS=10000 go test ./... -race -count=1

# Two of them, because they answer different questions and neither contains the
# other: golangci-lint is the idiom and correctness set, and modernize is "the
# stdlib grew a way to say this" — it ships with gopls rather than with
# golangci-lint, and it is the one that rewrites rather than only reports.
#
# modernize reports on stderr and exits 3 when it found something, so the run is
# judged by what is left after the noise is filtered rather than by the status.
# mutation.pb.go is filtered rather than fixed: it is protoc-gen-go's output and
# not this repository's code.
.PHONY: lint
lint: ## Run golangci-lint and gopls's modernize over the whole tree
	go run $(GOLANGCI) run --timeout 15m
	@out=$$(go run $(MODERNIZE) ./... 2>&1 | \
		grep -vE '\.pb\.go|^exit status|^go: (downloading|finding)|^#'); \
	if [ -n "$$out" ]; then printf 'modernize:\n%s\n' "$$out"; exit 1; fi
	@echo "modernize: clean"

# The same sweep, applied. modernize is the half that rewrites; golangci's
# --fix is narrower and is here because the formatters ride in it.
.PHONY: lint-fix
lint-fix: ## Apply what the linters can rewrite
	go run $(MODERNIZE) -fix ./... || true
	go run $(GOLANGCI) run --fix --timeout 15m

# The handbook's Markdown is the source and docs/handbook/site/ is generated
# from it; the check parses every mermaid block and resolves every link, which
# is the only way a broken one shows up before somebody opens the page.
#
# Two editions come out of it: site/ to browse, and site/waltz-handbook.html —
# one self-contained file — to attach to a release. WALTZ_REF is what the second
# one's links to source files point at, so a release build passes its tag.
.PHONY: handbook
handbook: ## Build both HTML editions of docs/handbook/ (needs node)
	cd docs/handbook && npm install --no-audit --no-fund && npm run check && npm run build

# The .pb.go file is checked in, so a clone builds and tests without protoc.
# This target is only for changing the WAL's record format — and changing it
# means changing what every existing log entry means, so read mutation.proto's
# header first. protoc-gen-go comes from go.mod's tool directive and needs no
# install; protoc itself does (apt: protobuf-compiler).
.PHONY: proto
proto: ## Regenerate mutation.pb.go from mutation.proto (needs protoc)
	@tmp=$$(mktemp -d); \
	printf '#!/bin/sh\nexec go tool protoc-gen-go "$$@"\n' > $$tmp/protoc-gen-go; \
	chmod +x $$tmp/protoc-gen-go; \
	PATH="$$tmp:$$PATH" protoc --go_out=. \
		--go_opt=module=github.com/aromanovich/waltz mutation/mutation.proto; \
	status=$$?; rm -rf $$tmp; exit $$status

# Reachability, not a dependency inventory: govulncheck reports an advisory only
# where a call path from this module's own code reaches the vulnerable symbol, so
# a green run is a claim about what waltz calls rather than about what it
# requires. Both halves of the answer matter to a deployment — what reaches one is
# the module versions this go.mod requires, raised through MVS, while the
# standard-library half is the toolchain they build with and the `toolchain` line
# here is only what waltz's own builds and CI use.
.PHONY: vuln
vuln: ## Check the module and the toolchain against the Go vulnerability database
	go run $(GOVULNCHECK) ./...

.PHONY: check
check: test race lint vuln ## The whole gate: the tests, the race detector, the linters and the advisories

.PHONY: help
help: ## List the targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
