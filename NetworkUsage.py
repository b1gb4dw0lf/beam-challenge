import apache_beam as beam
from apache_beam.pvalue import AsSingleton
from Utils import NetworkUsage, ParseNetworkLogs, MeanAndCount
import numpy as np


def filter_top10_network_usage(pipeline, file_path):
    with pipeline as p:
        def __custom_key(entry):
            # I'd expect it to sort on the value not the key
            # but have it your way, beam.
            # Negative-Step slicing for efficiency
            return entry[::-1]

        return (
            p
            | 'ReadInputText' >> beam.io.ReadFromText(file_path)
            | 'PrepareData' >> NetworkUsage()
            | 'CombineSameIPs' >> beam.CombinePerKey(sum)
            | 'Top10Usage' >> beam.combiners.Top.Of(10, key=__custom_key)
            | 'ConverToFlatMap' >> beam.FlatMap(lambda entry: entry)
        )


def filter_network_usage_by_mean(pipeline, file_path):
    with pipeline as p:
        fields = (
            p
            | 'ReadInputText' >> beam.io.ReadFromText(file_path)
            | 'ParseLogs' >> beam.ParDo(ParseNetworkLogs())
        )

        # Not sure I understand what AsSingleton does.
        global_mean = AsSingleton(
            fields
            | 'GetAllBytes' >> beam.Map(lambda elem: elem['http.response.bytes'])
            | 'CalculateGlobalMean' >> beam.combiners.Mean.Globally()
        )

        return (
            fields
            | 'MapIPBytePairs' >> beam.Map(lambda elem:
                                           (elem['source.ip'], elem['http.response.bytes']))
            | 'FilterBelowMean' >> beam.Filter(
                        lambda entry, mean: entry[1] > mean, global_mean)
            | 'CountPacketsOverMean' >> beam.combiners.Count.PerKey()
            # Using distinct was also a way to go, but I wanted to see how many
            # were above mean
            | 'SortByCount' >> beam.combiners.Top.Of(10, key=(lambda entry: entry[::-1]))
            | 'ConvertToFlatMap' >> beam.FlatMap(lambda entry: entry)
        )


def filter_by_std_dev(pipeline, file_path):
    with pipeline as p:
        def std_dev_map(entry, means_and_counts):
            # Creating variables probably slows the pipeline a lot
            # but keep it for readability for now.
            ip, entries = entry
            mean = means_and_counts[ip][0]
            count = means_and_counts[ip][1]
            # Use numpy for array-wise operations
            # TODO: Loading python for variance is probably not the best way to go
            # TODO: Feels like this can be a separate CombinePerKey
            variance = np.sum(np.square(np.array(entries) - mean)) / count
            return ip, np.sqrt(variance if variance > 0 else 0)

        def filter_by_stddev(entry, means_and_counts, std_devs):
            # Refraining from creating new variables during pipeline
            return entry[1] > \
                   (means_and_counts[entry[0]][0] + std_devs[entry[0]])

        fields = (
            p
            | 'ReadInputText' >> beam.io.ReadFromText(file_path)
            | 'ParseLogs' >> NetworkUsage()
        )

        # Combine mean and count to a single pipeline
        mean_and_count_per_key = beam.pvalue.AsDict(
            fields
            | 'GetMeanAndCountPerKey' >> beam.CombinePerKey(MeanAndCount())
        )

        # TODO: This part needs improvements
        std_dev_per_key = beam.pvalue.AsDict(
            fields
            | 'GroupByKey' >> beam.GroupByKey()
            | 'GetDifferencesPerKey' >> beam.Map(std_dev_map, mean_and_count_per_key)
        )

        return (
            fields
            | 'FilterZeroBytes' >> beam.Filter(lambda entry: entry[1] > 0)
            | 'FilterByStdDev' >> beam.Filter(
                                    filter_by_stddev, mean_and_count_per_key, std_dev_per_key)
            | 'GetIPs' >> beam.Keys()
            | 'GetDistinctIPs' >> beam.Distinct()
        )
