test:
	pytest tests.py

lint:
	flake8 --exclude greenstalk/__init__.py
	isort -m 3 -tc
