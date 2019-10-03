import logging
import sys
import os

import pandas as pd
from batch_geocoder import GeopyWrapper


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
root_dir = os.path.dirname(os.path.abspath(__name__))
storage_dir = os.path.join(root_dir, 'instance')

def get_df():
    return pd.DataFrame({'zipcode': ['16150', '16151', '16153', '16154',
        '16155', '16156', '16157', '16159']})

def test_batch():
    df = get_df()

    helper = GeopyWrapper(partition_size=2, storage_dir=storage_dir)
    helper.df = df.copy()
    assert not helper.df.empty

    helper.run()
    assert all([col in helper.df.columns for col in ['latitude', 'longitude']])

    assert len(df) == len(helper.df)
    logging.info('found: %s' % helper.df.latitude.isna().sum() / len(df))


if __name__ == '__main__':
    logging.info('initializing tests.')
    test_batch()
    logging.info('completed tests.')
