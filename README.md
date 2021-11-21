# transaction_filters_pipeline
Apache Beam Pipeline for transaction reports based on certain filters

## create virtual environment
### Windows:
python -m venv env
### Linux:
python3 -m venv env

## install requirements
pip install -r requirements.txt

## execute pipeline
### Windows:
python -m transaction_pipeline
### Linux:
python3 -m transaction_pipeline

## execute unit tests using pytest
pytest
