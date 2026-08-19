.PHONY: license
license: ## Add BUSL-1.1 license headers using addlicense
	@echo "Adding BUSL-1.1 license headers..."
	@command -v addlicense >/dev/null 2>&1 || (echo "addlicense not found. Install with: make install-addlicense" && exit 1)
	@addlicense -f LICENSE-HEADER.txt \
		-ignore "target/**" \
		-ignore "**/vendor/**" \
		.

.PHONY: license-check
license-check: ## Check if all Rust files have license headers
	@echo "Checking BUSL-1.1 license headers..."
	@command -v addlicense >/dev/null 2>&1 || (echo "addlicense not found. Install with: make install-addlicense" && exit 1)
	@addlicense -check -f LICENSE-HEADER.txt \
		-ignore "target/**" \
		-ignore "**/vendor/**" \
		. && echo "All files have license headers ✓" || \
		(echo "Some files are missing license headers. Run 'make license' to fix." && exit 1)

.PHONY: license-update
license-update: ## Update copyright year in headers
	@echo "Updating license headers with current year..."
	@find . -name "*.rs" -not -path "./target/*" -type f -exec sed -i 's/Copyright (c) [0-9]\{4\} M. Javani/Copyright (c) $(shell date +%Y) M. Javani/g' {} +
	@echo "License headers updated ✓"

.PHONY: install-addlicense
install-addlicense: ## Install addlicense tool
	go install github.com/google/addlicense@latest

.PHONY: build
build:
	cargo build --release
	strip --strip-all target/release/rzproxy
	upx --best --lzma target/release/rzproxy
	ls -lh target/release/rzproxy
	cp target/release/rzproxy .

.PHONY: help
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf " \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help