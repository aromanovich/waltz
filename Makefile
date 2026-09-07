.DEFAULT_GOAL := test

# Both linters are pinned here and run with `go run` rather than added to
# go.mod: a linter's dependency tree is not a build input for anything this
# module produces, and `tool` directives would put several hundred indirect
# requirements into the file a Temporal bump has to be readable in.
GOLANGCI := github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
MODERNIZE := golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@v0.23.0

# No `-p 1` here, and its absence is the deliberate half: nothing in this module
# wants a cluster, a container or a fixed port. Every backend lives in the test
# process and every database is keyed by a name minted per store, so the packages
# share nothing and run concurrently — including internal/verify/e2e, whose Temporal
# services take ports from the OS. `-count=1` is what keeps a green run from
# being yesterday's cache.
.PHONY: test
test: ## Run every test in the module (default target)
	go test ./... -count=1

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
.PHONY: handbook
handbook: ## Build the HTML edition of docs/handbook/ (needs node)
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

.PHONY: check
check: test lint ## The whole gate: the tests and the linters

.PHONY: help
help: ## List the targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
