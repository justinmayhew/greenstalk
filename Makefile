.PHONY: test
test:
	PYTHONWARNINGS=default pytest tests.py

.PHONY: test-with-coverage
test-with-coverage:
	PYTHONWARNINGS=default pytest --cov=greenstalk tests.py

.PHONY: lint
lint:
	black src/ tests.py
	isort --profile=black src/ tests.py
	flake8 src/ tests.py
	mypy --strict src/ tests.py

.PHONY: html-docs
html-docs:
	# This is the command readthedocs uses to build the site.
	cd docs && python3 -m sphinx -T -b html -d _build/doctrees -D language=en . _build/html

.PHONY: clean
clean:
	rm -rf .cache/ .coverage *.egg-info/ __pycache__/ **/*/__pycache__/ .tox/ .mypy_cache/ docs/_build/ .pytest_cache/ build/
