#!/bin/bash
echo
echo
echo "Running Pylint"
pylint *.py
echo
echo "Running Pydocstyle"
pydocstyle -v *.py
echo
echo "Running Prospector"
prospector *.py
echo "Cyclomatic Complexity"
radon cc *.py
echo "maintainability index"
radon mi *.py
echo "Running tests"
echo
pytest -v
echo "All done."