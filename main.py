import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from NetworkUsage import filter_top10_network_usage, filter_network_usage_by_mean, filter_by_std_dev
from UserAgentCheck import user_agent_threshold
import argparse


def main():
    options = PipelineOptions()
    pipeline = beam.Pipeline(options=options)

    parser = argparse.ArgumentParser(description="Choose input file and check")
    parser.add_argument('-input', '--input',
                        type=str,
                        action='store',
                        help='Path to input .jsonl file')
    parser.add_argument('-output', '--output',
                        type=str,
                        action='store',
                        required=False)
    parser_group = parser.add_mutually_exclusive_group(required=True)
    parser_group.add_argument('-top10', '--filterByTop10',
                              action='store_true', help="Display top 10 IPs that download most")
    parser_group.add_argument('-mean', '--filterByMean',
                              action='store_true', help="Display IPs that download more than global mean")
    parser_group.add_argument('-std', '--filterByStdDev',
                              action='store_true', help="Display IPs that download more than usual")
    parser_group.add_argument('-uat', '--filterByUserAgentThreshold',
                              action='store_true', help="Display IPs with more than one user agents")

    args = vars(parser.parse_args())
    action = None
    file_path = args['input']
    output_file_path = args['output']

    if args['filterByTop10']:
        action = filter_top10_network_usage
    elif args['filterByMean']:
        action = filter_network_usage_by_mean
    elif args['filterByStdDev']:
        action = filter_by_std_dev
    elif args['filterByUserAgentThreshold']:
        action = user_agent_threshold
    else:
        print('Shouldn\'t have came here :(')

    with pipeline as p:
        output = action(p, file_path)
        (
            output
            | 'Display' >> beam.Map(print)
        )

        if output_file_path:
            (
                output
                | beam.io.WriteToText(file_path)
            )


if __name__ == '__main__':
    main()
