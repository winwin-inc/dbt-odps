VERSION = 1.9.1rc1

# 默认目标
.DEFAULT_GOAL := all

all: tag build publish

tag:
	@echo "version = '$(VERSION)'" > src/dbt/adapters/odps/__version__.py
	@git add -u  
	@git commit -m "tag: v$(VERSION)"
	@git tag v$(VERSION)
	@git push --tags
	@echo "✅ Tagged version $(VERSION)"

build:
	@hatch clean
	@hatch build
	@echo "✅ Build completed"

publish:	
	@twine upload dist/*
	@echo "✅ Published to PyPI"

# 辅助目标
clean:
	@hatch clean
	@echo "✅ Cleaned build artifacts"

check-version:
	@echo "Current version: $(VERSION)"

help:
	@echo "Available targets:"
	@echo "  all      - Run tag, build, and publish (default)"
	@echo "  tag      - Update version, commit, and create git tag"
	@echo "  build    - Build the package"
	@echo "  publish  - Upload to PyPI"
	@echo "  clean    - Clean build artifacts"
	@echo "  help     - Show this help message"

.PHONY: all tag build publish clean check-version help