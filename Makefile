test:
	pytest tests.py

lint:
	flake8 greenstalk tests.py
	isort greenstalk tests.py --recursive --multi_line 5
	mypy greenstalk tests.py --ignore-missing-imports --strict-optional --disallow-untyped-defs
