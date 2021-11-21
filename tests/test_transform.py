# test_transform_successful
import pytest

from collections import namedtuple

import apache_beam as ap_beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from beam.transformation.transaction_filter_report import TransactionFilterReport
from beam.utils.write_to import format_result


Result = namedtuple("Result", ["date", "total_amount"])


@pytest.fixture
def setup():
    pass


def test_simple_successful_case():
    INPUT_TRANSACTION_DATA = [
        "2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99",
        "2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95",
        "2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22",
        "2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030",
        "2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,13700000023.08",
        "2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12",
        "2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,139.12",
    ]
    OUTPUT_TRANSACTION_DATA = [
        "2017-03-18, 2102.22",
        "2017-08-31, 13700000023.08",
        "2018-02-27, 268.24",
    ]
    with TestPipeline() as p:
        input = p | ap_beam.Create(INPUT_TRANSACTION_DATA)
        output = input | TransactionFilterReport()

        assert_that(output, equal_to(OUTPUT_TRANSACTION_DATA))


def test_simple_invalid_date_case():
    INPUT_TRANSACTION_DATA = [
        "2009-25-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99",
        "2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,139.12",
    ]
    with pytest.raises(ValueError) as ex:
        with TestPipeline() as p:
            input = p | ap_beam.Create(INPUT_TRANSACTION_DATA)
            output = input | TransactionFilterReport()

    assert (
        str(ex.value)
        == "time data '2009-25-09 02:54:25 UTC' does not match format '%Y-%m-%d %H:%M:%S %Z' [while running 'TransactionFilterReport/Split']"
    )


def test_simple_invalid_float_case():
    INPUT_TRANSACTION_DATA = [
        "2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,abc",
        "2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,139.12",
    ]
    with pytest.raises(ValueError) as ex:
        with TestPipeline() as p:
            input = p | ap_beam.Create(INPUT_TRANSACTION_DATA)
            output = input | TransactionFilterReport()

    assert (
        str(ex.value)
        == "could not convert string to float: 'abc' [while running 'TransactionFilterReport/Split']"
    )
