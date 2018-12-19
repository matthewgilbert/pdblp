help:
	@echo 'Make for some simple commands                          '
	@echo '                                                       '
	@echo ' Usage:                                                '
	@echo '     make lint          flake8 the codebase            '
	@echo '     make test          run unit tests                 '
	@echo '     make test_offline  run unit tests for parsing only'

lint:
	flake8 ./pdblp

test:
	pytest pdblp/tests -v

test_offline:
	pytest  pdblp/tests/ -v --offline
