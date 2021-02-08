import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.test_pipeline import TestPipeline
import unittest

from UserAgentCheck import user_agent_threshold


class UserAgentTest(unittest.TestCase):
    test_path = './mock/test.jsonl'

    def test_user_agent_threshold(self):
        with TestPipeline() as p:
            output = user_agent_threshold(p, self.test_path)
            assert_that(output, equal_to([('1.1.1.1', 2)]))


if __name__ == '__main__':
    unittest.main()
