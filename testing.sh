#!/bin/bash

if [[ -z $1 ]]; then
    echo "Please provide an argument"
    echo "Supported arguments:"
    echo "  - all"
    echo "  - test"
    exit 0
fi


if [[ $1 == "all" ]];then
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
    echo
    echo "maintainability index"
    radon mi lib/*.py
    radon mi *.py
    echo
    echo "Running mypy"
    python3 -m mypy tracker.py
    echo

    echo "Running pre-commit"
    pre-commit run  --all-files
    echo
    echo "Running tests"
    echo
    pytest -v --cov-report term-missing --cov='lib/' --cov='./tracker.py' -v
    echo "All done."
elif [[ $1 == "test" ]]; then
    pytest -v --cov-report term-missing --cov='lib/' --cov='./tracker.py' -v
else
    echo "Unsupported argument"
fi

