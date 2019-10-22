MSSQL Runner
==========

Redshift does not have stored procedures so we will use a simple script runner to execute our ELT and load tasks.

Both script runner and SQLWorkbenchJ will use the same notation for variable expansion: `$[?variable]`.  This string will be replaced at runtime by the appropriate variable:

There are three types of substitution parameters that can be used through script runner.  The first is a set of standard etl params for ease of use:

*  -f, from_date, default 1776-07-24
*  -t,to_date, default 9999-12-31
*  -b, batch_no, default is -1

The second option is for arbitrary variable expansion.  This is passed in the following format because rundeck commands don't like json quotes:

*  -p, example: -p "param1-val1, param2-val2"

The final option is to use the batchy integration

*  -wf, batchy_job, this will substitue parameters from a batchy workflow, this should be a fully qualified batchy job name of the format wf.job, if no job is specified it will assume global

Here is a sample SQL Script.  If run in SQL workbench you will be prompted for values for var1 and var2.

```
drop table if exists  zzztemp;

create table zzztemp (
  dt timestamp,
  var varchar,
  from_date timestamp,
  batch_no integer
  );

insert into zzztemp
values (getdate(), '$[?var1]', '$[?from_date]', '$[?batch_no]');

insert into zzztemp
values (getdate(), '$[?var2]', '$[?from_date]', '$[?batch_no]');

select * from zzztemp;

```

In the script runner you would use the following params to substitute that value.  It is assumed these params would be dynamically substituted by the calling script or informatica process:

`python -m mssql_runner.module -s "sample/mssql_runner_test.sql" -p "var1-cat, var2-dog" -b '9999'`

Assuming you had workflow config in batchy under wf3, you could also use this script:

`python -m  mssql_runner.module -s sample/mssql_runner_test.sql -wf wf3`
