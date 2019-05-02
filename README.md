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

NOTE: Because requirements.txt lists https as the address for the repos, you'll get prompted for a password. Because of the way accounts are managed you're likely using SSH keys to auth and might not know your password. To get around this you can create an "App Password" to auth. https://confluence.atlassian.com/bitbucket/app-passwords-828781300.html#Apppasswords-Createanapppassword See the directions here for setting that up: https://bitbucket.org/equinoxfitness/datacoco/src/master/

You may also need to install PostgreSQL:
```
brew install postgresql
```
Attempt to run the rsqoop runner. Or look into rsqoop_runner folder for more details

```
python -m rsqoop_runner.module -sc data_admin -tc cosmo -st dbo.budget_tierx_budget -tt edw_landing.stg_budget_tierx_budget
```
