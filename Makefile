VERSION = 1.9.1-alpha.7

tag:
	echo "version = '$(VERSION)'" > src/dbt/adapters/odps/__version__.py
	git add -u  
	git commit -m "tag: v$(VERSION)"
	git tag v$(VERSION)
	git push --tags

build:
	hatch clean
	hatch build


publish:	
	twine upload dist/*
 
	
all: tag build publish

