test:
	pytest --cov=greenstalk tests.py

lint:
	flake8 greenstalk.py tests.py
	isort greenstalk.py tests.py --multi-line 5
	mypy greenstalk.py tests.py --ignore-missing-imports --strict-optional --disallow-untyped-defs
