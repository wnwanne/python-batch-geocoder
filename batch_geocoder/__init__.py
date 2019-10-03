from pgeocode import Nominatim
from .pgeocode_country_codes import pgeocode_country_codes
import pandas as pd
import numpy as np

import logging
import traceback
import os
import sys

__version__ = 'v0.1'
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class PgeocodeWrapper:
    def __init__(self, partition_size, storage_dir, country='US'):
        """requires separate inputs for each country"""
        self.partition_size = partition_size
        self.storage_dir = storage_dir
        self.df = pd.DataFrame()
        if country in list(pgeocode_country_codes):
            self.country_code = pgeocode_country_codes[country]
        elif country.upper() in list(pgeocode_country_codes.values()):
            self.country_code = country
        logging.info('initializing with country_code %s.' % self.country_code)
        self.geolocator = Nominatim(self.country_code)

    def geocode(self, x):
        """takes zipcode"""
        locations = self.geolocator.query_postal_code(x)
        return locations.latitude, locations.longitude

    def manage_partitioning(self, partition_number):
        start = self.start_i
        end = start + self.partition_size
        partition = self.df[start:end].copy()

        _zips = partition.zipcode.tolist()
        latitudes, longitudes = self.geocode(_zips)

        partition['latitude'] = latitudes.tolist()
        partition['longitude'] = longitudes.tolist()
        filename = '%s_part_%s.csv' % (self.country_code, partition_number)
        filepath = os.path.join(self.storage_dir, filename)
        logging.info('Saving %s to %s' % (partition.shape, filepath))
        partition.to_csv(filepath, index=False)
        self.start_i += self.partition_size
        return list(latitudes), list(longitudes)

    def run(self):
        if self.df.empty:
            pass
        self.start_i = 0
        n_partitions = int(np.ceil(len(self.df)/self.partition_size))
        latitudes, longitudes = [], []

        try:
            for i in range(n_partitions):
                lats, lons = self.manage_partitioning(i)
                latitudes += lats
                longitudes += lons

            self.df['latitude'] = latitudes
            self.df['longitude'] = longitudes
        except Exception as e:
            logging.error('Partition failed. Error: %s' % e)
            traceback.print_exc() # for initial dev TODO: create debug mode
