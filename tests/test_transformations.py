import pytest

import angelou.sparksession as S
import angelou.transformations as T

class TestTransformations(object):

    def test_with_greeting(self):
        source_data = [
            ("jose", 1),
            ("li", 2)
        ]
        source_df = S.spark.createDataFrame(
            source_data,
            ["name", "age"]
        )

        actual_df = T.with_greeting(source_df)

        expected_data = [
            ("jose", 1, "hello!"),
            ("li", 2, "hello!")
        ]
        expected_df = S.spark.createDataFrame(
            expected_data,
            ["name", "age", "greeting"]
        )

        assert(expected_df.collect() == actual_df.collect())

