import unittest
from pyspark.sql import SparkSession
from main.data_transformation import calculate_age_and_classify, mask_email_domain
import logging

# Assuming the functions `calculate_age_and_classify` and `mask_email_domain` are already defined
# from your_module import calculate_age_and_classify, mask_email_domain

# Set up logging for testing
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up the Spark session for testing."""
        cls.spark = SparkSession.builder \
            .appName("TestFunctions") \
            .getOrCreate()

    def test_calculate_age_and_classify(self):
        """Test the calculate_age_and_classify function."""
        # Sample test data (birthday column)
        data = [("1990-01-01",),
                ("2005-05-15",),
                ("1960-07-20",),
                ("1950-12-30",)]

        # Create DataFrame
        df = self.spark.createDataFrame(data, ["birthday"])

        # Call the calculate_age_and_classify function (assumed to be imported)
        result_df = calculate_age_and_classify(df)

        # Collect the results
        result = result_df.collect()

        # Expected age groups after calculation
        # Assuming the current date is 2024-11-28
        expected = [
            ("1990-01-01", 34, "[30-40]"),
            ("2005-05-15", 19, "[10-20]"),
            ("1960-07-20", 64, "[60-70]"),
            ("1950-12-30", 74, "[70-80]")
        ]

        # Assert that the calculated results match the expected output
        for idx, row in enumerate(result):
            self.assertEqual(row["age"], expected[idx][1])
            self.assertEqual(row["age_group"], expected[idx][2])

    def test_mask_email_domain(self):
        """Test the mask_email_domain function."""
        # Sample test data (email column)
        data = [("john.doe@example.com",),
                ("jane.smith@domain.com",),
                ("bob@example.org",),
                ("alice@company.net",)]

        # Create DataFrame
        df = self.spark.createDataFrame(data, ["email"])

        # Call the mask_email_domain function (assumed to be imported)
        masked_df = mask_email_domain(df)

        # Collect the results
        result = masked_df.collect()

        # Expected masked emails
        expected = [
            ("****@example.com",),
            ("****@domain.com",),
            ("****@example.org",),
            ("****@company.net",)
        ]

        # Assert that the masked email results match the expected output
        for idx, row in enumerate(result):
            self.assertEqual(row["email"], expected[idx][0])

    @classmethod
    def tearDownClass(cls):
        """Stop the Spark session after all tests."""
        cls.spark.stop()


# Run the test case
if __name__ == '__main__':
    unittest.main()
