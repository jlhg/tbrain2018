#!/usr/bin/env python
# Usage:
# python product_sum.py --project $project --runner DataflowRunner \
#   --staging_location gs://${bucket}/staging --temp_location gs://${bucket}/temp \
#   --zone asia-east1-a --input gs://${bucket}/query_log.csv \
#   --output gs://${bucket}/output/product_sum/product_sum

import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


def split(element):
    data = element.strip().split(',')
    file_id = data[0]
    customer_id = data[3]
    return (file_id, product_id)

def format_result(element):
    (file_id, product_ids) = element
    return '%s,%s' % (file_id, len(list(product_ids)))

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
            | 'Group' >> beam.GroupByKey()
            | 'FormatResult' >> beam.Map(format_result)
        )
        results | 'WriteToText' >> WriteToText(known_args.output)


if __name__ == '__main__':
  run()
