RSQOOP
==========

Truncate and load, stage data from mssql to posgres/redshift.

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
Next, prepare the etl.cfg file in the root directory of the project.

```
[mssql key name]
db_name=
user=
server=
password=<encoded in base64>
port=1433
type=mssql

i.e
[WebDB]
db_name=WebDatabase
user=any_user
server=websql
password=YW55IHBhc3N3b3Jk
port=1433
type=mssql

[redshift/postgres key name]
db_name=
user=
host=
port=
password=<encoded in base64>
type=

i.e
[cosmo]
db_name=cosmo
user=admin
host=any.host.com
port=5439
password=YW55IHBhc3N3b3Jk
type=postgres


[general]
temp_bucket = <s3 temp folder>
temp_key = <s3 temp folder key>
aws_access_key=
aws_secret_key=
env=test
aws_region=us-east-1
```

# rSqoop

A simple utility for syncing database tables from MSSQL into Redshift. It gets it's name from the Hadoop tool Sqoop which sync's relational tables into Hadoop.

You can run rSqoop by importing the class to a wrapper script or executing the module itself

Wrapper Example:

```python
#!/usr/bin/env python

from rsqoop_runner.module import rSqoop

rSqoop('life','cosmo').stage_to_redshift('mstr.d_channel', 'edw_landing.d_channel')
rSqoop('life','cosmo').stage_to_redshift('mstr.d_facility', 'edw_landing.d_facility')
rSqoop('life','cosmo').stage_to_redshift('mstr.d_date', 'edw_landing.d_date')
rSqoop('life','cosmo').stage_to_redshift('mstr.d_employee', 'edw_landing.d_employee')
```

Module Example:

`python -m rsqoop_runner.module -sc data_admin -tc cosmo -st dbo.budget_tierx_budget -tt edw_landing.stg_budget_tierx_budget`
* -sc, source connection, a mssql db name corresponding to an entry in the config file
* -tc, target connection, usually redshift database
* -st, source table(s), space separated list of mssql tables to stage
* -tt, target table(s), space separated list of redshift tables to stage
* -sf, select source table fields. -q (remove-quotes) is required when running -sf

