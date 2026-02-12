# SentinelSync Makefile
# Basic targets for development

.PHONY: help install lint test clean

help:
	@echo "📋 SentinelSync Development Commands"
	@echo ""
	@echo "  make install     - Install Python dependencies"
	@echo "  make lint        - Run code linters"
	@echo "  make test        - Run unit tests"
	@echo "  make test-cov    - Run tests with coverage"
	@echo "  make clean       - Clean build artifacts"
	@echo ""

install:
	@echo "📦 Installing Python dependencies..."
	pip install --upgrade pip
	pip install -r requirements.txt
	pip install black flake8 mypy pytest pytest-cov
	@echo "✅ Dependencies installed!"

lint:
	@echo "🔍 Running linters..."
	black --check src/ tests/
	flake8 src/ tests/
	mypy src/ --ignore-missing-imports
	@echo "✅ Linting complete!"

test:
	@echo "🧪 Running unit tests..."
	pytest tests/ -m "not integration" -v
	@echo "✅ Tests complete!"

test-cov:
	@echo "📊 Running unit tests with coverage..."
	pytest tests/ -m "not integration" --cov=src --cov-report=term --cov-report=html
	@echo "✅ Coverage report generated!"

clean:
	@echo "🧹 Cleaning build artifacts..."
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name "htmlcov" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name "build" -exec rm -rf {} +
	find . -type d -name "dist" -exec rm -rf {} +
	@echo "✅ Clean complete!"