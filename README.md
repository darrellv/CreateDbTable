# CreateDbTable
Console app that will create a database table and insert records all from the same input file.

First, checks to see if there are any tables in the connected database, if so, it writes to the error log and stops.

next, it reads the file in.  first line is table name, 2nd line is columns, the rest of the file is the data to be inserted.

next it sets up the columns by scanning the data to see which columns are nvarchar, int, decimal, or date.

it then generates a create table script and runs it against the database.  after that it will insert the records, skipping those that
generate errors

finally, it writes out the error log if there were any errors and ends.
