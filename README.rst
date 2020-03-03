Maximilian 🤖
============

.. image:: https://api.codacy.com/project/badge/Grade/92ef60d1ddd840ce830a486fe5521b0e
    :target: https://www.codacy.com/manual/equinoxfitness/maximilian?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=equinoxfitness/maximilian&amp;utm_campaign=Badge_Grade
    :alt: Code Quality Grade

.. image:: https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg
    :target: https://github.com/equinoxfitness/maximilian/blob/master/CODE_OF_CONDUCT.rst
    :alt: Code of Conduct

Maximilian is a repository of ETLs and APIs for working with Equinox's AWS data environment.

Quick Start
------------

Sample run for rsqoop runner (look into `rsqoop_runner <https://github.com/equinoxfitness/maximilian/tree/master/rsqoop_runner>`_ folder for more details)
::

    python -m rsqoop_runner.module -sc source_connection -tc target_connection -st source_table -tt target_table

Sample run for mssql runner (look into `mssql_runner <https://github.com/equinoxfitness/maximilian/tree/master/mssql_runner>`_ folder for more details)
::

    python -m mssql_runner.module -s "sample/mssql_runner_test.sql" -p "var1-cat, var2-dog" -b '9999'

Sample run for script runner (look into `script_runner <https://github.com/equinoxfitness/maximilian/tree/master/script_runner>`_ folder for more details)
::

    python -m script_runner.module -s "sample/script_runner_test.sql" -p "var1-cat, var2-dog" -f '1980-12-31 07:00' -b '9999'

Installation
------------

maximilian requires Python 3.6+

::

    git clone https://github.com/equinoxfitness/maximilian.git

Prerequisites
-------------

You may need to install PostgreSQL:
::

    brew install postgresql

Setting up
----------

1.  Create virtual environment named **venv**
::

    cd maximilian
    python3 -m venv venv

2.  Activate virtual environment
::

    source venv/bin/activate

3.  Install any dependencies (this will install them into your virtual environment).
Note: if you are installing psycopg2 on windows use [this](http://www.stickpeople.com/projects/python/win-psycopg/).
::

    pip install -r requirements.txt

4.  Prepare the **etl.cfg** file in the root directory of the project.

::

    [mssql key name]
    db_name=
    user=
    server=
    password=<encoded in base64>
    port=1433
    type=mssql

    [redshift/postgres key name]
    db_name=
    user=
    host=
    port=
    password=<encoded in base64>
    type=

    [general]
    temp_bucket = <s3 temp folder>
    temp_key = <s3 temp folder key>
    aws_access_key=
    aws_secret_key=
    env=test
    aws_region=us-east-1
::

**Example etl.cfg**
::

    [My_Mssql]
    db_name=My_Mssql
    user=any_user
    server=websql
    password=YW55IHBhc3N3b3Jk
    port=1433
    type=mssql

    [My_Redshift]
    db_name=My_Redshift
    user=admin
    host=any.host.com
    port=5439
    password=YW55IHBhc3N3b3Jk
    type=postgres

    [general]
    temp_bucket = my_bucket
    temp_key = my_key
    aws_access_key= AKAASDLAFJKMADEUP
    aws_secret_key= YYAKAaldjkasfMADEUP
    env=test
    aws_region=us-east-1

Development
-----------

Testing
~~~~~~~

::

    pip install -r requirements-dev.txt

Modify the connection configuration for integration testing.

To run the testing suite, simply run the command: ``python -m unittest discover tests``

For coverage report, run ``tox`` View the results in
.tox/coverage/index.html

Contributing
~~~~~~~~~~~~

Contributions to Maximilian are welcome!

Please reference guidelines to help with setting up your development
environment
`here <https://github.com/equinoxfitness/maximilian/blob/master/CONTRIBUTING.rst>`__.