#!/usr/bin/env python
# Usage:
# python query_times.py --project $project --runner DataflowRunner \
#   --staging_location gs://${bucket}/staging --temp_location gs://${bucket}/temp \
#   --zone asia-east1-a --input gs://${bucket}/query_log.csv \
#   --output gs://${bucket}/output/query_times/query_times

import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


def split(element):
    data = element.strip().split(',')
    file_id = data[0]
    return file_id

def format_result(element):
    (file_id, count) = element
    return '%s,%s' % (file_id, count)

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        required=True,
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | 'ReadFromText' >> ReadFromText(known_args.input)
        results = (
            lines
            | 'Split' >> beam.Map(split)
            | beam.combiners.Count.PerElement()
            | 'FormatResult' >> beam.Map(format_result)
        )
        results | 'WriteToText' >> WriteToText(known_args.output)


if __name__ == '__main__':
  run()
