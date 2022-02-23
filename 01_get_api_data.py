from kaggle.api.kaggle_api_extended import KaggleApi
import os
from zipfile import ZipFile
from datetime import datetime
import numpy as np
import pandas as pd
import random
np.random.seed(42)

url = "yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018"

# download kaggle data: airline-delay-and-cancellation, 2018.csv
start = datetime.now()

api = KaggleApi()
api.authenticate()
api.dataset_download_file(url, "2018.csv")

end = datetime.now()

print(f"Data download execution time: {end - start}")

# unzip file
zf = ZipFile('2018.csv.zip')
zf.extractall()
zf.close()
os.rename('2018.csv', 'rawdata_2018.csv')

# files size
file_size_zipped = os.path.getsize("./2018.csv.zip") / (1024 * 1024)
file_size_unzipped = os.path.getsize("./rawdata_2018.csv") / (1024 * 1024)
print(f"Zipped file size: {round(file_size_zipped)} MB")
print(f"Unzipped file size: {round(file_size_unzipped)} MB")

# get random csv sample for map reduce tests

p = 0.001  # % of data
df = pd.read_csv("rawdata_2018.csv", header=0, skiprows=lambda i: i > 0 and random.random() > p)
df.to_csv("test_2018.csv", index=False, header=False)

# write columns names to txt
columns = df.columns.to_list()

print(columns)

for col in columns:
    with open("headers_2018.txt", "a", ) as file:
        if col == columns[-1]:
            file.write(str(col.lower())[0:-4])
        else:
            file.write(str(col.lower()) + ', ')

# delete headers in rawdata.csv
file = open("rawdata_2018.csv", "r")
lines = file.readlines()

file = open("rawdata_2018.csv", "w")
file.writelines(lines[1:])
file.close()
