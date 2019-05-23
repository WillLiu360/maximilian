rSqoop
=======

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

