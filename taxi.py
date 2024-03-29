import argparse
import datetime
import matplotlib.pyplot as plt
import pandas as pd
import requests
from pathlib import Path

"""To scale up the data pipeline the function load_data_frame could be """
"""altered to only load chunks which fit into the memory of the machine. In """
"""get_rolling_mean one would then have to store the number of trips and """
"""their total sum for each chunk, such that the total average for each """
"""window can be computed after all chunks have been processed. A similar """
"""setup could be used to parallelize or distribute the calculation."""


def year_month_in_interval(anchor_date: datetime.datetime,
                           min_delta: pd.Timedelta,
                           max_delta: pd.Timedelta) -> [(int, int)]:
    """Returns the list of (year, month) in time interval"""
    ym = []
    min_year, max_year = ((anchor_date - min_delta).year,
                          (anchor_date + max_delta).year)
    min_month, max_month = ((anchor_date - min_delta).month,
                            (anchor_date + max_delta).month)
    y, m = min_year, min_month
    while y < max_year or (y == max_year and m <= max_month):
        ym.append((y, m))
        m += 1
        if m == 13:
            m = 1
            y += 1
    return ym


def download_taxi_data(year: int, month: int, output: str):
    """Download NYC taxi data and store in output file"""
    url = 'https://s3.amazonaws.com/nyc-tlc/trip+data/'
    filename = f'yellow_tripdata_{year:04}-{month:02}.csv'
    response = requests.get(url + filename, allow_redirects=True)
    response.raise_for_status()
    with open(output, 'wb') as file:
        file.write(response.content)


def validate_data_files(ym_list: [(int, int)]) -> [str]:
    """Check if data files already exist, otherwise download them"""
    file_names = []
    for year, month in ym_list:
        file_name = f'yellow_tripdata_{year:04}-{month:02}.csv'
        if not Path(file_name).is_file():
            download_taxi_data(year, month, file_name)
        file_names.append(file_name)
    return file_names


def filter_by_interval(df: pd.DataFrame, anchor_date: datetime.datetime,
                       min_delta: pd.Timedelta,
                       max_delta: pd.Timedelta) -> pd.DataFrame:
    """Filter data by taxi pickup date to match time period"""
    return df[(df.tpep_pickup_datetime - anchor_date >= -min_delta)
              & (df.tpep_pickup_datetime - anchor_date < max_delta)]


def load_data_frame(file_names: [str], anchor_date: datetime.datetime,
                    min_delta: pd.Timedelta,
                    max_delta: pd.Timedelta) -> pd.DataFrame:
    """Fill the data frame with data from the data files"""
    df = pd.concat((pd.read_csv(f, usecols=['tpep_pickup_datetime',
                                            'trip_distance'],
                                parse_dates=['tpep_pickup_datetime'],
                                infer_datetime_format=True)
                   for f in file_names))
    df = filter_by_interval(df, anchor_date, min_delta, max_delta)
    return df


def get_rolling_mean(df: pd.DataFrame, anchor_date: datetime.datetime,
                     min_delta: pd.Timedelta, max_delta: pd.Timedelta,
                     window: pd.Timedelta, step: pd.Timedelta) -> [float]:
    """Calculate the mean of the intervals [n*step, n*step+window)"""
    rolling_mean = []
    it_date = anchor_date - min_delta
    while it_date + window <= anchor_date + max_delta:
        chunk = filter_by_interval(df, it_date, pd.to_timedelta('0'),
                                   window)
        rolling_mean.append(chunk['trip_distance'].mean())
        it_date += step
    return rolling_mean


def plot_rolling_mean(data: [float]):
    """Plot rolling mean"""
    plt.plot(range(len(data)), data)
    plt.show()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('anchor_date',
                        help=("anchor date of the total time interval "
                              "[anchor_date-min_delta, anchor_date+max_delta)"
                              ", e.g. '2019-03-01 00:00:00'"))
    parser.add_argument('min_delta',
                        help="lower bound of the time period in days, e.g. 0")
    parser.add_argument('max_delta',
                        help="upper bound of the time period in days, e.g. 60")
    parser.add_argument('window',
                        help="sliding window size in days, e.g. 45")
    parser.add_argument('step',
                        help="step size of the sliding window in days, e.g. 1")
    parser.add_argument('--plot', help="generate a plot of the rolling mean",
                        action='store_true')
    args = parser.parse_args()

    anchor_date = pd.to_datetime(args.anchor_date)
    min_delta = pd.to_timedelta(float(args.min_delta), unit='d')
    max_delta = pd.to_timedelta(float(args.max_delta), unit='d')
    window = pd.to_timedelta(float(args.window), unit='d')
    step = pd.to_timedelta(float(args.step), unit='d')

    if any(td < pd.to_timedelta('0') for td in (min_delta, max_delta)):
        print("Time bounds must be non-negative.")
        return
    if any(td < pd.to_timedelta('0') for td in (window, step)):
        print("Time windows must be positive.")
        return

    ym_list = year_month_in_interval(anchor_date, min_delta, max_delta)
    file_names = validate_data_files(ym_list)
    df = load_data_frame(file_names, anchor_date, min_delta, max_delta)
    rolling_mean = get_rolling_mean(df, anchor_date, min_delta,
                                    max_delta, window, step)
    print(rolling_mean)
    if args.plot:
        plot_rolling_mean(rolling_mean)


if __name__ == "__main__":
    main()
