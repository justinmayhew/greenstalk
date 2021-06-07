test:
	PYTHONWARNINGS=default pytest tests.py

test-with-coverage:
	PYTHONWARNINGS=default pytest --cov=greenstalk tests.py

lint:
	flake8 src/ tests.py
	isort src/ tests.py --multi-line 5
	mypy src/ tests.py --ignore-missing-imports --strict-optional --disallow-untyped-defs

clean:
	rm -rf .cache/ .coverage *.egg-info/ __pycache__/ .tox/ .mypy_cache/ docs/_build/
