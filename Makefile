help:
	@echo 'Make for some simple commands                          '
	@echo '                                                       '
	@echo ' Usage:                                                '
	@echo '     make lint          flake8 the codebase            '
	@echo '     make test          run unit tests                 '
	@echo '     make test_parse    run unit tests for parsing only'

lint:
	flake8 ./pdblp

test:
	pytest pdblp/tests -v

test_parse:
	pytest  pdblp/tests/test_parser.py -v
