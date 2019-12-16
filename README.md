# NYC yellow taxi data pipeline
This is a toy code that generates the rolling average of the trip length of NYC yellow taxis. The data is taken from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page. The code uses the start date of the trip as its time stamp. The rolling average is defined by a sliding window of size `window` and step size `step`. The rolling average is computed over the total time period [`anchor_date`-`min_delta`, `anchor_date`+`max_delta`).

Run `python taxi.py --help` to print the possible command line arguments:
```
$ python taxi.py --help
usage: taxi.py [-h] [--plot] anchor_date min_delta max_delta window step

positional arguments:
  anchor_date  anchor date of the total time interval [anchor_date-min_delta,
               anchor_date+max_delta), e.g. '2019-03-01 00:00:00'
  min_delta    lower bound of the time period, e.g. '0 days'
  max_delta    upper bound of the time period, e.g. '60 days'
  window       sliding window size, e.g. '45 days'
  step         step size of the sliding window, e.g. '1 day'

optional arguments:
  -h, --help   show this help message and exit
  --plot       generate a plot of the rolling mean
```
Example command line arguments to produce the rolling average over a 45 day window with a step size of 1 day in the total time period of 90 days from the start date 2019-03-01 00:00:00:
```
$ python taxi.py '2019-03-01 00:00:00' '0' '90 days' '45 day' '1 day'
[2.992670148197292, 2.9938750859480248, ..., 3.02796146922059, 3.0219090805514086]
```
The associated unit tests can be run using the python unittest framework:
```
$ python -m unittest -v taxi_test
test_data_frame_loading (taxi_test.TestTaxiMethods) ... ok
test_file_validation (taxi_test.TestTaxiMethods) ... ok
test_interval_filter (taxi_test.TestTaxiMethods) ... ok
test_rolling_mean (taxi_test.TestTaxiMethods) ... ok
test_year_month_list (taxi_test.TestTaxiMethods) ... ok

----------------------------------------------------------------------
Ran 5 tests in 69.462s

OK
```
