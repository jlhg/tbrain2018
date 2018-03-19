#!/usr/bin/env python
import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def split(element):
    data = element.strip().split(',')
    file_id = data[0]
    product_id = data[3]
    return (file_id, product_id)

def min_value(element):
    (file_id, product_ids) = element
    count_per_element = {}
    for product_id in product_ids:
        if product_id not in count_per_element:
            count_per_element[product_id] = 1
        else:
            count_per_element[product_id] += 1
    result = min(list(count_per_element.values()))
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
            | 'Min' >> beam.Map(min_value)
            | 'FormatResult' >> beam.Map(format_result)
        )
        results | 'WriteToText' >> WriteToText(known_args.output)


if __name__ == '__main__':
  run()
