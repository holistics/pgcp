# pgcp - Copy PostgreSQL tables between databases (for analytics purpose)

pgcp is a simple tool to copy tables from a source postgres DB to a destination postgres DB.


The main use case of the tool is to make a dump of data from production databases
 to a centralized database for analytics purpose. Thus don't expect the copy to be an exact copy,
 we do drop a lot of table's metadata and only retain what we think is important for analytics

Example:

    pgcp -s db1 -d db2 -t public.users public.bookings

## Usage

Copy all tables in schema public to destination database, retaining the schema name

    pgcp -s db1 -d db2 -t public.*

Copy all tables in schema public to destination database, but to a different schema

    pgcp -s db1 -d db2 -t public.* --force-schema



