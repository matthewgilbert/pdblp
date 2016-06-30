help:
	@echo 'Make for some simple commands        '
	@echo '                                     '
	@echo ' Usage:                              '
	@echo '     make lint    flake8 the codebase'
	@echo '     make test    run unit tests     '

lint:
	flake8 ./pdblp

test:
	python -m unittest pdblp.tests.test_pdblp
