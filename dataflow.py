import argparse
import json
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def filter_missing(element):
    return all(value != 'None' for key, value in element.items() if key != 'time')


def convert_measurements(element):
    element['pressure'] = float(element['pressure']) / 6.895  # Convert pressure from kPa to psi
    element['temperature'] = float(element['temperature']) * 1.8 + 32  # Convert temperature from Celsius to Fahrenheit
    return element


def run(argv=None):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--input', dest='input', required=True,
                        help='Input Pub/Sub topic to read measurement readings.')
    parser.add_argument('--output', dest='output', required=True,
                        help='Output Pub/Sub topic to write processed measurements to.')
    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        measurements = (p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=known_args.input)
                        | "Decode JSON" >> beam.Map(lambda x: json.loads(x.decode('utf-8'))))

        filtered_measurements = measurements | 'Filter missing measurements' >> beam.Filter(filter_missing)

        converted_measurements = filtered_measurements | 'Convert measurements' >> beam.Map(convert_measurements)

        (converted_measurements | 'Encode JSON' >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
                               | 'Write to Pub/Sub' >> beam.io.WriteToPubSub(topic=known_args.output))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()