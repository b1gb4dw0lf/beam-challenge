from Utils import ParseNetworkLogs
import apache_beam as beam


def user_agent_threshold(pipeline, file_path):
    with pipeline as p:
        return (
            p
            | beam.io.ReadFromText(file_path)
            | 'ParseNetworkLogs' >> beam.ParDo(ParseNetworkLogs())
            | 'MapIPsToUserAgents' >> beam.Map(lambda elem:
                                               (elem['source.ip'], elem['agent.id']))
            | 'GetDistinct' >> beam.Distinct()
            | 'Group' >> beam.combiners.Count.PerKey()
            | 'Filter' >> beam.Filter(lambda entry: entry[1] > 1)
        )


def user_agent_windowed(pipeline, file_path):
    """
    This probably needs the use of windows and timestamps
    """
    raise NotImplementedError
