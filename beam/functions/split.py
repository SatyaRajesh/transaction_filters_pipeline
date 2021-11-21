import logging
import apache_beam as beam
from datetime import datetime

from beam.schema.transaction import Transaction
from beam.utils.constants import TRANSACTION_INPUT_DATE_FORMAT
from apache_beam.error import BeamError, PValueError, RuntimeValueProviderError


class Split(beam.DoFn):
    def process(self, element: str):
        try:
            timestamp, origin, destination, transaction_amount = element.split(",")
            # See if we need to work with Timezones
            return [
                Transaction(
                    datetime.strptime(timestamp, TRANSACTION_INPUT_DATE_FORMAT),
                    float(transaction_amount),
                )
            ]
        except ValueError as ve:
            logging.error(
                f"Split Class generated ValueError for Data: {element} Message: {ve}"
            )
            raise ve
        except OverflowError as oe:
            logging.error(
                f"Split Class generated OverflowError for Data: {element} Message: {oe}"
            )
            raise oe
        except PValueError as pe:
            logging.error(
                f"Split Class generated PValueError for Data: {element} Message: {pe}"
            )
            raise pe
        except RuntimeValueProviderError as re:
            logging.error(
                f"Split Class generated RuntimeValueProviderError for Data: {element} Message: {re}"
            )
            raise re
        except BeamError as be:
            logging.error(
                f"Split Class generated BeamError for Data: {element}  Message: {be}"
            )
            raise be
