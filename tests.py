import logging
import sys
import os

import pandas as pd
from batch_geocoder import PgeocodeWrapper


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
root_dir = os.path.dirname(os.path.abspath(__name__))
storage_dir = os.path.join(root_dir, 'instance')

def get_df():
    return pd.DataFrame({'zipcode': ['L4E 0S4', '16151', '16153', '16154',
        '16155', '16156', '16157', '16159']})

def split_canada_and_us(df):
    """assumes anythin numeric is US, else is canada. returns canada, us"""
    is_canada = ~df.zipcode.str.isdigit()
    is_us = df.zipcode.str.isdigit()
    return df[is_canada].copy(), df[is_us].copy()

def test_batches():
    df = get_df()
    cad_df, us_df = split_canada_and_us(df)

    cad_helper = PgeocodeWrapper(partition_size=2, storage_dir=storage_dir,
        country='ca')
    us_helper = PgeocodeWrapper(partition_size=2, storage_dir=storage_dir,
        country='us')

    cad_helper.df = cad_df.copy()
    us_helper.df = us_df.copy()

    cad_helper.run()
    us_helper.run()

    result = pd.concat([us_helper.df.dropna(), cad_helper.df.dropna()])
    assert all([col in result.columns for col in ['latitude', 'longitude']])

    assert len(df) == len(result)
    logging.info('found: %s' % (~result.latitude.isna()).sum())


if __name__ == '__main__':
    logging.info('initializing tests.')
    test_batches()
    logging.info('completed tests.')
