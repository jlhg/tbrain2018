#!/usr/bin/env python
import argparse
import apache_beam as beam
import numpy
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def split(element):
    data = element.strip().split(',')
    file_id = data[0]
    customer_id = data[1]
    return (file_id, customer_id)

def std(element):
    (file_id, customer_ids) = element
    count_per_element = {}
    for customer_id in customer_ids:
        if customer_id not in count_per_element:
            count_per_element[customer_id] = 1
        else:
            count_per_element[customer_id] += 1
    result = numpy.std(list(count_per_element.values()))
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
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | 'ReadFromText' >> ReadFromText(known_args.input)
        results = (
            lines
            | 'Split' >> beam.Map(split)
            | 'Group' >> beam.GroupByKey()
            | 'StandardDeviation' >> beam.Map(std)
            | 'FormatResult' >> beam.Map(format_result)
        )
        results | 'WriteToText' >> WriteToText(known_args.output)


if __name__ == '__main__':
  run()
