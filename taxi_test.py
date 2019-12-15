import pandas as pd
import taxi
import unittest


class TestTaxiMethods(unittest.TestCase):

    def setUp(self):
        self.path = ['test_tripdata.csv']
        self.anchor_date = pd.to_datetime('2019-03-01 00:00:00')
        self.min_delta = pd.to_timedelta('0')
        self.max_delta = pd.to_timedelta('1 hour')
        self.df = taxi.load_data_frame(self.path, self.anchor_date,
                                       self.min_delta, self.max_delta)

    def test_year_month_list(self):
        min_delta = pd.to_timedelta('90 days')
        max_delta = pd.to_timedelta('60 days')
        ym = taxi.year_month_in_interval(self.anchor_date, min_delta,
                                         max_delta)
        expected = [(2018, 12), (2019, 1), (2019, 2), (2019, 3), (2019, 4)]
        self.assertEqual(ym, expected)

    def test_file_validation(self):
        path = taxi.validate_data_files([(2019, 3), (2019, 4)])
        self.assertEqual(path, ['yellow_tripdata_2019-03.csv',
                         'yellow_tripdata_2019-04.csv'])

    def test_data_frame_loading(self):
        self.assertEqual(self.df.shape, (8, 2))

    def test_interval_filter(self):
        df = taxi.filter_by_interval(self.df, self.anchor_date+self.max_delta/2,
                                     self.max_delta/4, self.max_delta/2)
        self.assertEqual(df.shape, (4, 2))

    def test_rolling_mean(self):
        window = pd.to_timedelta('20 min')
        step = pd.to_timedelta('10 min')
        rolling_mean = taxi.get_rolling_mean(self.df, self.anchor_date,
                                             self.min_delta, self.max_delta,
                                             window, step)
        expected = [4.165, 1.415, 1.85, 5.4, 5.4]
        self.assertTrue(len(rolling_mean) == len(expected))
        for i in range(len(rolling_mean)):
            self.assertAlmostEqual(rolling_mean[i], expected[i])


if __name__ == "__main__":
    unittest.main()
