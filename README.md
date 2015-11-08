# pgcp - Copy PostgreSQL tables between databases (for analytics purpose)

pgcp is a simple tool to copy tables from a source postgres DB to a destination postgres DB.


The main use case of the tool is to make a dump of data from production databases
 to a centralized database for analytics purpose. Don't expect the copy to be an exact copy,
 we do drop a lot of table's metadata and only retain what we think is important for analytics.


What we retain:

* Table's schema structure
* Table's indexes
* Table's data

What we don't clone over (non-exhaustive):

* Sequences (auto increment)
* Triggers
* ...

Requirements:

* Make sure you have `psql` installed on the machine running `pgcp`

## Installation


Install gem from RubyGems

    gem install pgcp


Create file `~/.pgcp.yml` that contains the credentials of your interested databases:

    databases:
      production_db:
        user: postgres
        password:
        dbname: 
        host: your_production.server.com
      analytics_db:
        user: postgres
        dbname: analytics
        password:
        host: your_analytics.server.com


## Usage

For usage details, runs: `pgcp help`

Copy a single `bookings` table from production to analytics:

    pgcp -s production_db -d analytics_db -t public.bookings 

Copy multiple tables:

    pgcp -s production_db -d analytics_db -t public.bookings public.registrations


Copy all tables in schema public to destination database

    pgcp -s production_db -d analytics_db -t public.*

Copy all tables in schema public to destination database, but to a different schema

    pgcp -s production_db -d analytics_db -t public.* --force-schema
