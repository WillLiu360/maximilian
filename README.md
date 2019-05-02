Maximilian3
==========

Maximilian is a repository of ETLs and APIs for working with Equinox's AWS data environment.

![Maximilian](docs/max.jpeg)


* install any dependencies (this will install them into your virtual environment)
Note: if you are installing psycopg2 on windows use this: http://www.stickpeople.com/projects/python/win-psycopg/

```bash
$ pip install -r requirements.txt
```

NOTE: requirements.txt will install ALL requirements, individual installs per module can be found in /requirements

You may also need to install PostgreSQL:
```
brew install postgresql
```
Attempt to run the rsqoop runner. Or look into rsqoop_runner folder for more details

```
python -m rsqoop_runner.module -sc data_admin -tc cosmo -st dbo.budget_tierx_budget -tt edw_landing.stg_budget_tierx_budget
```
