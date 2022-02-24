
# Airline Delay and Cancellation 2018 Dataset Analysis
Big data analysis exercise using Python, MapReduce paradigm, Hadoop and AWS.

## Table of content

* [Repository content](#Repository-content)
* [Tech Stack And Requirements](#Tech-Stack-And-Requirements)
* [How to use:](#How-to-use)
    - [Download Kaggle dataset](#my-anchor-1)
    - [Run job scripts in Terminal/PowerShell locally](#2.-Run-job-scripts-in-Terminal/PowerShell-locally:)
    - [Run jobs in AWS (S3, EMR-Hadoop Core)](#my-anchor-2)

## Repository content
- ./output/ (job output csv files)

- 01_get_api_data.py (download dataset script)

- map reduce jobs:
    -   02_total_distance_per_month.py 

    - 03_total_airline_distance.py

    - 04_avg_delay_per_month.py

    - 05_avg_delay_per_airlines.py

    - 06_cancelled_flights_rate_per_month.py

    - 07_cancelled_flights_rate_per_airlines.py

- README.md

- airlines.csv (airline name/code mapper file)

- data_description.py (data exploratory job)

- test_2018_analysis.ipynb (data sample analysis)

## Tech Stack And Requirements

- win10
- python 3.9 (os, random, zipfile, datetime, pandas, kaggle, mrjob, awscli)
- PowerShell 5.1
- Kaggle Account, Kaggle API
- AWS Account (S3, EMR)
- IDE: Pycharm, Jupyter Notebook


## How to use
In order to use the project:
- download and unpack airline_delay_and_cancellation_analysis 
- make sure you meet the requirements from section [Tech Stack And Requirements](#Tech-Stack-And-Requirements)
- run from terminal/powershell

### 1. Download Kaggle dataset:

Requires Kaggle Account and Kaggle API username and key:

    For more information visit:
    https://www.kaggle.com/docs/

In order to download dataset run script **01_get_api_data.py**

Script provides:
- **2018.csv.zip** -  "Airline Delay and Cancellation 2018" zipped file
- **rawdata_2018.csv** - unpack dataset
- **headers_2018.txt** - columns headers
- **test_2018.csv** - random sample for tests


### 2. Run job scripts in Terminal/PowerShell locally: {#my-anchor-1}

Python files - 02 to 07 contain map reduce jobs.

Navigate to folder and type in command line:

- for "overall" jobs (files 02,04,06):

        python .\02_total_distance_per_month.py .\test_2018.csv

- for "airline" jobs (files 03, 04, 06) use flag **--airlines** (to map airline company name from *airlines.csv*)

        python .\03_total_airline_distance.py .\test_2018.csv --airlines .\airlines.csv

- save job locally:

        python .\02_total_distance_per_month.py .\test_2018.csv  > .\output\02_total_distance_per_month.py
        python .\03_total_airline_distance.py .\test_2018.csv --airlines .\airlines.csv > .\output\03_total_airline_distance.py

- script to change encoding:

        python .\07_cancelled_flights_rate_per_airlines.py .\test_2018.csv --airlines .\airlines.csv | out-file -encoding UTF8 -filepath .\output\07_cancelled_flights_rate_per_airlines.csv

### 3. Run jobs in AWS (S3, EMR-Hadoop Core): {#my-anchor-2}
Requires AWS Account and installed AWS Command Line Interface (AWS CLI):

        pip install awscli

- configure PowerShell with AWS *Access key ID* and *Secret key* (IAM User Guide)
- create Amazon S3 bucket and upload raw dataset

        https://aws.amazon.com/getting-started/hands-on/backup-to-s3-cli/

PowerShell scripts:
- check dataset availability:
        
        aws s3 ls s3://airlines_delay_usa_dp/data/airline_delay_usa_2018

- navigate to local *airline* folder and run cluster in AWS EMR (1 instance, instance type = m5.xlarge)

        python .\02_total_distance_per_month.py -r emr s3://airlines_delay_usa_dp/data/airline_delay_usa_2018 --output-dir=s3://airlines_delay_usa_dp/output/02_total_distance_per_month

- run with 4 machines (1 master and 3 core nodes,  instance type = m5.xlarge)

    --num-core-instances 4

        python .\02_total_distance_per_month.py -r emr --num-core-instances 4






