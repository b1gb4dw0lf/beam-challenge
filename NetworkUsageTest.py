import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.test_pipeline import TestPipeline
import unittest

from NetworkUsage import filter_top10_network_usage, \
    filter_network_usage_by_mean, filter_by_std_dev


class NetworkUsageTest(unittest.TestCase):
    test_path = './mock/test.jsonl'

    def test_top10_usage(self):
        with TestPipeline() as p:
            output = filter_top10_network_usage(p, self.test_path)
            assert_that(
                output,
                equal_to([
                    ('1.1.1.1', 56),
                    ('1.1.1.2', 24),
                    ('1.1.1.3', 22),
                    ('1.1.1.4', 21),
                    ('1.1.1.7', 7),
                    ('1.1.1.5', 4),
                    ('1.1.1.6', 3),
                    ('1.1.1.9', 1),
                    ('1.1.1.8', 0)
                ])
            )

    def test_mean_usage(self):
        with TestPipeline() as p:
            output = filter_network_usage_by_mean(p, self.test_path)
            assert_that(
                output,
                equal_to([
                    ('1.1.1.1', 8),
                    ('1.1.1.4', 2),
                    ('1.1.1.3', 2),
                    ('1.1.1.2', 2),
                    ('1.1.1.7', 1)
                ])
            )

    def test_std_dev_usage(self):
        with TestPipeline() as p:
            output = filter_by_std_dev(p, self.test_path)
            assert_that(
                output,
                equal_to([
                    '1.1.1.2',
                    '1.1.1.3',
                    '1.1.1.4'
                ])
            )


if __name__ == '__main__':
    unittest.main()

