.DEFAULT_GOAL := help

PYTHONPATH=
SHELL=bash
ifeq ($(VENV),)
VENV := .venv
endif

ifeq ($(OS),Windows_NT)
	VENV_BIN=$(VENV)/Scripts
else
	VENV_BIN=$(VENV)/bin
endif

.PHONY: .venv
.venv:  ## Set up virtual environment and install requirements
	python3 -m venv $(VENV)
	$(VENV_BIN)/pip install --upgrade pip
	$(VENV_BIN)/pip install -r requirements-dev.txt

.PHONY: requirements
requirements: .venv  ## Install/refresh Python project requirements
	$(VENV_BIN)/pip install --upgrade pip
	$(VENV_BIN)/pip install -r requirements-dev.txt

.PHONY: requirements-all
requirements-all: .venv  ## Install/refresh all Python requirements (including optional dependencies)
	$(VENV_BIN)/pip install --upgrade pip
	$(VENV_BIN)/pip install -r requirements-dev.txt
	$(VENV_BIN)/pip install -e ".[all]"

.PHONY: build
build: .venv  ## Compile and install Spatio for development
	$(VENV_BIN)/maturin develop

.PHONY: build-release
build-release: .venv  ## Compile and install Spatio binary with optimizations
	$(VENV_BIN)/maturin develop --release

.PHONY: build-debug
build-debug: .venv  ## Compile and install Spatio binary with debug symbols
	$(VENV_BIN)/maturin develop --profile dev

.PHONY: install
install: build  ## Install the package in development mode
	$(VENV_BIN)/pip install -e .

.PHONY: wheel
wheel: .venv  ## Build wheel for distribution
	$(VENV_BIN)/maturin build --release
	@echo "Wheel built in target/wheels/"

.PHONY: sdist
sdist: .venv  ## Build source distribution
	$(VENV_BIN)/maturin sdist
	@echo "Source distribution built in target/wheels/"

.PHONY: publish-test
publish-test: wheel  ## Publish to Test PyPI
	$(VENV_BIN)/twine upload --repository testpypi target/wheels/*

.PHONY: publish
publish: wheel  ## Publish to PyPI
	$(VENV_BIN)/twine upload target/wheels/*

.PHONY: lint
lint: .venv  ## Run lint checks (only)
	$(VENV_BIN)/ruff check src/ tests/ examples/
	$(VENV_BIN)/mypy src/ tests/

.PHONY: fmt
fmt: .venv  ## Run autoformatting (and lint)
	$(VENV_BIN)/ruff check --fix src/ tests/ examples/
	$(VENV_BIN)/ruff format src/ tests/ examples/
	-$(VENV_BIN)/mypy src/ tests/

.PHONY: clippy
clippy:  ## Run clippy on Rust code
	cargo clippy -- -D warnings -D clippy::dbg_macro

.PHONY: fmt-rust
fmt-rust:  ## Format Rust code
	cd .. && cargo fmt --all

.PHONY: pre-commit
pre-commit: fmt clippy fmt-rust  ## Run all code formatting and lint/quality checks

.PHONY: test
test: .venv build  ## Run fast unittests
	$(VENV_BIN)/pytest -n auto --tb=short

.PHONY: test-all
test-all: .venv build  ## Run all tests including slow ones
	$(VENV_BIN)/pytest -n auto --tb=short -m "slow or not slow"

.PHONY: test-unit
test-unit: .venv build  ## Run unit tests only
	$(VENV_BIN)/pytest -n auto --tb=short -m "unit"

.PHONY: test-integration
test-integration: .venv build  ## Run integration tests only
	$(VENV_BIN)/pytest -n auto --tb=short -m "integration"

.PHONY: test-benchmark
test-benchmark: .venv build-release  ## Run benchmark tests
	$(VENV_BIN)/pytest --benchmark-only -v

.PHONY: coverage
coverage: .venv build  ## Run tests and report coverage
	$(VENV_BIN)/pytest --cov=spatio --cov-report=html --cov-report=term-missing
	@echo "Coverage report generated in htmlcov/"

.PHONY: examples
examples: .venv build  ## Run all examples
	@echo "Running basic usage example..."
	$(VENV_BIN)/python examples/basic_usage.py
	@echo "Running trajectory example..."
	$(VENV_BIN)/python examples/trajectory_tracking.py
	@echo "Running performance demo..."
	$(VENV_BIN)/python examples/performance_demo.py

.PHONY: docs
docs: .venv  ## Build documentation
	$(VENV_BIN)/mkdocs build

.PHONY: docs-serve
docs-serve: .venv  ## Serve documentation locally
	$(VENV_BIN)/mkdocs serve

.PHONY: docs-clean
docs-clean:  ## Clean documentation build
	rm -rf site/

.PHONY: security
security: .venv  ## Run security checks
	$(VENV_BIN)/bandit -r src/

.PHONY: typecheck
typecheck: .venv  ## Run type checking
	$(VENV_BIN)/mypy src/ tests/

.PHONY: profile
profile: .venv build-release  ## Profile performance with py-spy
	$(VENV_BIN)/python -c "import spatio; print('Profiling spatio...')"
	@echo "Run: py-spy record -o profile.svg -- python your_script.py"

.PHONY: benchmark
benchmark: .venv build-release  ## Run comprehensive benchmarks
	$(VENV_BIN)/python examples/benchmark_suite.py

.PHONY: clean
clean:  ## Clean up caches and build artifacts
	@rm -rf .pytest_cache/
	@rm -rf .mypy_cache/
	@rm -rf .ruff_cache/
	@rm -rf htmlcov/
	@rm -rf site/
	@rm -rf dist/
	@rm -rf *.egg-info/
	@rm -f .coverage
	@rm -f coverage.xml
	@rm -f profile.svg
	@find . -type f -name '*.py[co]' -delete
	@find . -type d -name __pycache__ -exec rm -r {} +
	@find . -name "*.so" -delete
	@find . -name "*.pyd" -delete
	cd .. && cargo clean

.PHONY: clean-all
clean-all: clean  ## Clean everything including virtual environment
	rm -rf $(VENV)

.PHONY: check
check: lint typecheck security  ## Run all checks (lint, type, security)

.PHONY: ci
ci: check test coverage  ## Run full CI pipeline

.PHONY: dev-setup
dev-setup: .venv requirements build  ## Complete development setup
	$(VENV_BIN)/pre-commit install
	@echo "Development environment ready!"
	@echo "Activate with: source $(VENV_BIN)/activate"

.PHONY: version
version:  ## Show current version
	@$(VENV_BIN)/python -c "import spatio; print(f'spatio {spatio.__version__}')"

.PHONY: info
info: .venv  ## Show development environment info
	@echo "Python: $$($(VENV_BIN)/python --version)"
	@echo "Pip: $$($(VENV_BIN)/pip --version)"
	@echo "Virtual env: $(VENV)"
	@echo "Rust: $$(rustc --version)"
	@echo "Cargo: $$(cargo --version)"

.PHONY: help
help:  ## Display this help screen
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' | sort
