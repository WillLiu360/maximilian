Maximilian
==========

Maximilian is a repository of ETLs and APIs for working with Equinox's AWS data environment.

![Maximilian](docs/max.jpeg)

## Getting Started

Clone repository
```
$ git clone https://github.com/equinoxfitness/maximilian.git
```

### Prerequisites

You may also need to install PostgreSQL:
```
brew install postgresql
```

### Installing

Setup virtual environment:
Install virtualenv
```
$ pip install virtualenv
```
Create virtual environment named **venv**
```
$ cd maximilian
$ virtualenv --no-site-packages venv
```
Activate virtual environment
```
$ source venv/bin/activate
```
Install any dependencies (this will install them into your virtual environment). Note: if you are installing psycopg2 on windows use this: http://www.stickpeople.com/projects/python/win-psycopg/
```
$ pip install -r requirements.txt
```
Note: requirements.txt will install ALL requirements, individual installs per module can be found in /requirements

## Running the tests

```
$ python -m unittest -v tests
```

Sample run for rsqoop runner (look into [rsqoop_runner](https://github.com/equinoxfitness/maximilian/tree/master/rsqoop_runner) folder for more details)
```
$ python -m rsqoop_runner.module -sc source_connection -tc target_connection -st source_table -tt target_table
```
Sample run for mssql runner (look into [mssql_runner](https://github.com/equinoxfitness/maximilian/tree/master/mssql_runner) folder for more details)
```
$ python -m mssql_runner.module -s "sample/mssql_runner_test.sql" -p "var1-cat, var2-dog" -b '9999'
```
Sample run for script runner (look into [script_runner](https://github.com/equinoxfitness/maximilian/tree/master/script_runner) folder for more details)
```
$ python -m script_runner.module -s "sample/script_runner_test.sql" -p "var1-cat, var2-dog" -f '1980-12-31 07:00' -b '9999'
```

## Deployment



## Contributing

Please read [CONTRIBUTING.md](https://github.com/equinoxfitness/maximilian/blob/master/CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [Jenkins](https://jenkins.io/) for versioning. For the versions available, see the [tags on this repository](https://github.com/equinoxfitness/maximilian/tags). 

## Contributors

See the list of [contributors](https://github.com/equinoxfitness/maximilian/contributors) who participated in this project.
