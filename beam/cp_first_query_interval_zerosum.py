#!/usr/bin/env python
import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def interval_zerosum(array):
    if len(array) <= 1:
        return 0

    count = 0
    sorted_array = sorted(array)
    for i in range(len(sorted_array)):
        if i == len(sorted_array) - 1:
            break
        if sorted_array[i + 1] - sorted_array[i] == 0:
            count += 1
    return count

def split(element):
    data = element.strip().split(',')
    file_id = data[0]
    timestamp = int(data[2])
    cp_id = '%s:%s' % (data[1], data[3])
    return (file_id, (cp_id, timestamp))

def zerosum(element):
    (file_id, pairs) = element

    cp_timestamps = {}
    for cp_id, timestamp in pairs:
        if cp_id in cp_timestamps:
            cp_timestamps[cp_id].append(timestamp)
        else:
            cp_timestamps[cp_id] = [timestamp]

    first_timestamps = []
    for cp_id, timestamps in cp_timestamps.items():
        first_timestamps.append(sorted(timestamps)[0])

    result = interval_zerosum(first_timestamps)
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
            | 'CPFirstIntervalZerosum' >> beam.Map(zerosum)
            | 'FormatResult' >> beam.Map(format_result)
        )
        results | 'WriteToText' >> WriteToText(known_args.output)


if __name__ == '__main__':
  run()
