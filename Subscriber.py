import argparse, logging, re, json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions, GoogleCloudOptions

subscription = "projects/hazem-data-engineer/subscriptions/pubsub_subcription"
project_id = "hazem-data-engineer"


class Split(beam.DoFn):

    def process(self, element):
        element = json.loads(element)
        return [{
            'deviceId': element["deviceId"],
            'temperature': element["temperature"],
            'longitude': element["location"][0],
            'latitude': element["location"][1],
            'time': element["time"]
        }]


def run(argv=None):
    """Build and run the pipeline."""
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    p = beam.Pipeline(options=options)

    # Big query configs
    table_spec = "hazem-data-engineer:miniprojects.temp_data"
    table_schema = "deviceId:STRING,temperature:FLOAT,longitude:FLOAT,latitude:FLOAT,time:TIMESTAMP"

    # Read from PubSub into a PCollection.
    messages = (
            p | 'Read From PubSub' >> beam.io.ReadFromPubSub(subscription=subscription).with_output_types(bytes)
            | 'Decoding' >> beam.Map(lambda x: x.decode('utf-8'))
            | 'Extract elements' >> beam.ParDo(Split().with_output_types('unicode'))
            | 'Write into Bigtable' >> beam.io.WriteToBigQuery(
                    table_spec,
                    schema=table_schema,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
