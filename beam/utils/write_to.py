from inspect import _void
from apache_beam.io import WriteToText
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.pvalue import PCollection

from beam.utils.constants import (
    TRANSACTION_OUTPUT_HEADER,
    TRANSACTION_OUTPUT_GZIP_FORMAT,
)
from beam.schema.transaction import Transaction


def write_to_gzip(collection: PCollection, output_prefix: str):
    collection | WriteToText(
        file_path_prefix=output_prefix,
        # file_name_suffix=str(datetime.timestamp(datetime.now())) + ".jsonl.gz",
        file_name_suffix=TRANSACTION_OUTPUT_GZIP_FORMAT,
        shard_name_template="",
        compression_type=CompressionTypes.GZIP,
        header=TRANSACTION_OUTPUT_HEADER,
    )


# Format the transactions into a PCollection of strings.
def format_result(trans_data: Transaction):
    return "%s, %s" % (trans_data.date, trans_data.total_amount)
