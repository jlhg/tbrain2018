#!/usr/bin/env python
import argparse
import apache_beam as beam
import numpy
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from scipy import stats

def split(element):
    data = element.strip().split(',')
    file_id = data[0]
    cp_id = '%s:%s' % (data[1], data[3])
    return (file_id, cp_id)

def geomean(element):
    (file_id, cp_ids) = element
    count_per_element = {}
    for cp_id in cp_ids:
        if cp_id not in count_per_element:
            count_per_element[cp_id] = 1
        else:
            count_per_element[cp_id] += 1
    counts = numpy.array(list(count_per_element.values()))
    result = stats.gmean(counts[counts != 0])
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
            | 'GeometricMean' >> beam.Map(geomean)
            | 'FormatResult' >> beam.Map(format_result)
        )
        results | 'WriteToText' >> WriteToText(known_args.output)


if __name__ == '__main__':
  run()
