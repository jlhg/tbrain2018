#!/usr/bin/env python
# Usage:
# python customer_query_mean.py --project $project --runner DataflowRunner \
#   --staging_location gs://${bucket}/staging --temp_location gs://${bucket}/temp \
#   --zone asia-east1-a --input gs://${bucket}/query_log.csv \
#   --output gs://${bucket}/output/customer_query_mean/customer_query_mean

import argparse
import apache_beam as beam
import numpy
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


def split(element):
    data = element.strip().split(',')
    file_id = data[0]
    customer_id = data[1]
    return (file_id, customer_id)

def mean(element):
    (file_id, customer_ids) = element
    result = (
        customer_ids
        | 'CountPerElement' >> beam.combiners.Count.PerElement()
        | 'CalculateMean' >> beam.Map(lambda x: numpy.mean(list(x[1])))
    )
    return (file_id, result)

def format_result(element):
    (file_id, value) = element
    return '%s,%s' % (file_id, value)

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
            | 'Mean' >> beam.Map(mean)
            | 'FormatResult' >> beam.Map(format_result)
        )
        results | 'WriteToText' >> WriteToText(known_args.output)


if __name__ == '__main__':
  run()
