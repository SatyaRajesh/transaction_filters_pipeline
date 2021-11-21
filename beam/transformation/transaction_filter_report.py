import logging
import apache_beam as beam
from beam.functions.split import Split
from beam.utils.constants import (
    TRANSACTION_AMOUNT_FILTER,
    TRANSACTION_DATE_FILTER,
    TRANSACTION_OUTPUT_DATE_FORMAT,
)
from beam.utils.write_to import format_result
from apache_beam.pvalue import PCollection
from apache_beam.error import (
    BeamError,
    TransformError,
    PValueError,
    RuntimeValueProviderError,
)


class TransactionFilterReport(beam.PTransform):
    def expand(self, p_coll: PCollection):
        # Transform logic goes here.
        try:
            return (
                p_coll
                | "Split" >> beam.ParDo(Split())
                | "Filter Transaction Amount less than 20 (default)"
                >> beam.Filter(
                    lambda trans: trans.transaction_amount > TRANSACTION_AMOUNT_FILTER
                )
                | "Filter Transactions happened before 2010 (default)"
                >> beam.Filter(
                    lambda trans: int(trans.timestamp.strftime("%Y"))
                    >= TRANSACTION_DATE_FILTER
                )
                | "Sum For each date"
                >> beam.GroupBy(
                    date=lambda trans: trans.timestamp.strftime(
                        TRANSACTION_OUTPUT_DATE_FORMAT
                    )
                ).aggregate_field("transaction_amount", sum, "total_amount")
                | "Format to CSV" >> beam.Map(format_result)
            )
        except TransformError as te:
            logging.error(
                f"TransactionFilterReport Transformation generated TransformError Message: {te}"
            )
            raise te
        except PValueError as pe:
            logging.error(
                f"TransactionFilterReport Transformation generated PValueError Message: {pe}"
            )
            raise pe
        except RuntimeValueProviderError as re:
            logging.error(
                f"TransactionFilterReport Transformation generated RuntimeValueProviderError Message: {re}"
            )
            raise re
        except BeamError as be:
            logging.error(
                f"TransactionFilterReport Transformation generated BeamError Message: {be}"
            )
            raise be
