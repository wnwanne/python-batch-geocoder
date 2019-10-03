from geopy.geocoders import Nominatim
import pandas as pd
import numpy as np

import logging
import traceback
import os
import sys


logging.basicConfig(stream=sys.stdout, level=logging.INFO)


class GeopyWrapper:
    def __init__(self, partition_size, storage_dir):
        self.partition_size = partition_size
        self.storage_dir = storage_dir
        self.df = pd.DataFrame()
        self.geolocator = Nominatim(user_agent='GeopyWrapper')

    def geocode(self, x):
        """takes zipcode"""
        logging.info('geocoding %s' % x)
        location = self.geolocator.geocode(x)
        return (location.latitude, location.longitude)

    def manage_partitioning(self, partition_number):
        start = self.start_i
        end = start + self.partition_size
        partition = self.df[start:end].copy()

        geocodes = []
        for i in range(len(partition)):
            _zip = partition.zipcode.iloc[i]
            geocodes.append(self.geocode(_zip))

        partition['latitude'] = [vals[0] for vals in geocodes]
        partition['longitude'] = [vals[1] for vals in geocodes]
        filename = 'partition_%s.csv' % partition_number
        filepath = os.path.join(self.storage_dir, filename)
        logging.info('Saving %s to %s' % (partition.shape, filepath))
        partition.to_csv(filepath, index=False)
        self.start_i += self.partition_size
        return geocodes

    def run(self):
        if self.df.empty:
            pass
        self.start_i = 0
        n_partitions = int(np.ceil(len(self.df)/self.partition_size))
        geocodes = []

        try:
            for i in range(n_partitions):
                geocodes += self.manage_partitioning(i)
            self.df['latitude'] = [vals[0] for vals in geocodes]
            self.df['longitude'] = [vals[1] for vals in geocodes]
        except Exception as e:
            logging.error('Partition failed. Error: %s' % e)
            traceback.print_exc() # for initial dev TODO: create debug mode
