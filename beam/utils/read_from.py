import apache_beam as beam
from apache_beam.io import ReadFromText


def read_from_file(pipeline: beam.Pipeline, file_pattern: str):
    return pipeline | ReadFromText(file_pattern, skip_header_lines=1)
