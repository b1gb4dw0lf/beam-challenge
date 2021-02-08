import logging

import apache_beam as beam
import json
from dateutil import parser


class ParseNetworkLogs(beam.DoFn):
    def __init__(self):
        beam.DoFn.__init__(self)

    def process(self, elem, *args, **kwargs):
        try:
            entry = json.loads(elem)
            yield {
                'source.ip': entry['source.ip'],
                # Is integer overflow an issue in Python3?
                # TODO: check best practice to store total_bytes
                'http.response.bytes': int(entry['http.response.bytes']),
                # Beam requires seconds since epoch to use withTimestamps
                # '@timestamp': parser.parse(entry['@timestamp']).timestamp(),
                'agent.id': entry['agent.id']
            }
        except Exception as ex:
            print("Failed to format {}", entry)
            print(ex)


class NetworkUsage(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | 'ParseLogs' >> beam.ParDo(ParseNetworkLogs())
            | 'MapIPBytePairs' >> beam.Map(lambda elem:
                                           (elem['source.ip'], elem['http.response.bytes']))
        )


class MeanAndCount(beam.CombineFn):
    """
    Use this class to calculate mean and count on one go
    """
    def create_accumulator(self, *args, **kwargs):
        return 0.0, 0  # sum, count

    def add_input(self, mutable_accumulator, element, *args, **kwargs):
        total, count = mutable_accumulator
        return total + element, count + 1

    def merge_accumulators(self, accumulators, *args, **kwargs):
        total, count = zip(*accumulators)
        return sum(total), sum(count)

    def extract_output(self, accumulator, *args, **kwargs):
        total, count = accumulator
        if count:
            return (total/count), count
        else:
            return float('Nan'), 0
