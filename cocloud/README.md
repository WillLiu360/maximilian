# Datacoco3 - Cloud

Equinox Common Code Utility for Python 3 for cloud interactions!
There are currently interaction classes for the following AWS services:

+ Athena
+ CloudWatch Logs
+ ECS
+ EMR
+ S3

In addition there is a Informatica Cloud Interaction class.


## Installing the latest version

```
pip install git+https://bitbucket.org/equinoxfitness/datacoco3.cloud.git#egg=cocloud
```

## Development

#### Getting Started

It is recommended to use the steps below to set up a virtual environment for development:

```
python3 -m venv <virtual env name>
source venv/bin/activate
pip install -r requirements.txt
```

#### Testing
prepare etl.cfg at the root folder using key and values from `tests/test_data/test.cfg` or get it from `https://s3.console.aws.amazon.com/s3/buckets/eqxdl-prod-support/datacoco3-tests/cloud`

Run test options:
1. using Pycharm
2. run test and coverage using tox in your terminal

View the results in .tox/coverage/index.html




