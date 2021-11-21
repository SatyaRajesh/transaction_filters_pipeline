import argparse
import logging

import apache_beam as beam
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.error import BeamError, PipelineError, RunnerError

from beam.schema.transaction import Transaction
from beam.transformation.transaction_filter_report import TransactionFilterReport
from beam.utils.read_from import read_from_file
from beam.utils.write_to import write_to_gzip


def parse_validate_arguments(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        default="gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv",
        help="Input CSV file to process.",
    )
    parser.add_argument(
        "--output",
        dest="output",
        default="output/results",
        help="Output file to write results to.",
    )
    parser.add_argument(
        "--trans_amount_gt",
        dest="trans_amount_gt",
        default=20,
        help="Filter transaction amount greater than this value",
    )
    parser.add_argument(
        "--exclude_trans_before_year",
        dest="exclude_trans_before_year",
        default=2010,
        help="Filter transaction amount greater than this value",
    )
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend(
        [
            "--runner=DirectRunner",
            "--project=VirginMediaBeamTest",
            "--region=europe-west2",
            "--staging_location=gs://cloud-samples-data/bigquery/sample-transactions/",
            "--temp_location=gs://cloud-samples-data/bigquery/sample-transactions/",
            "--job_name=transaction-filter-job",
        ]
    )
    return known_args, pipeline_args


def execute_pipeline(argv=None, save_main_session=True):
    """Main entry point; defines and runs the beam pipeline."""

    known_args, pipeline_args = parse_validate_arguments(argv)
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as pipeline:
        try:
            csv_lines = read_from_file(pipeline, known_args.input)
            csv_data = (
                csv_lines | "Transformation Pipeline" >> TransactionFilterReport()
            )
            write_to_gzip(csv_data, known_args.output)
        except PipelineError as pe:
            logging.error(f"execute_pipeline generate Pipeline Error Message: {pe}")
            raise PipelineError
        except RunnerError as re:
            logging.error(f"execute_pipeline generate RunnerError Message: {re}")
            raise re
        except BeamError as be:
            logging.error(f"execute_pipeline generate Beam Error Message: {be}")
            raise be
        except Exception as e:
            logging.error(f"Generic Exception: {e}")
            raise e


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    execute_pipeline()
