#!/bin/bash
echo
echo
echo "Running Pylint"
pylint lib/*.py
pylint *.py
echo
echo "Running Pydocstyle"
pydocstyle -v lib/*.py
pydocstyle -v *.py
echo
echo "Running Prospector"
prospector lib/*.py
prospector *.py
echo "Cyclomatic Complexity"
radon cc lib/*.py
radon cc *.py
echo "maintainability index"
radon mi lib/*.py
radon mi *.py
echo "Running tests"
echo
pytest -v tests/* --cov-report term-missing --cov='lib/' --cov='./tracker.py' -v
echo "All done."
