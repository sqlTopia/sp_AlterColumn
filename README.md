sp_AlterColumn accepts batch processing on multiple columns, even changing the database collation.

As the name implies, this is a procedure that helps developers and DBAs to alter columns in many different ways.
The problem sp_AlterColumn solves is when you need to change a column datatype (for example INT to BIGINT) and there are a lot of foreign keys, indexes, computed columns, check constraints, legacy data type rules to mention a few, that prevent you from doing a single ALTER COLUMN.

sp_AlterColumn will find all connected columns using foreign keys. It will find all problematic objects and ultimately create a number of T-SQL statements that are processed in the correct order to do your changes. sp_AlterColumn can be run with multiple executors, using atac_process procedure. Start as many instances as you need, they will never block each other and idle time will be a absolute minimum.

sp_AlterColumn even have built in checks to see if the new collation will be problematic for current indexes. It will display the key and all duplicate values.
