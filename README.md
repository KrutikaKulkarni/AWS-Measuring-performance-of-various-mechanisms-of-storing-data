# AWS-Measuring-performance-of-various-mechanisms-of-storing-data

Various mechanisms for storing data (files, RDBs-SQL, “no-SQL”, in-memory,
hybrid schemes) are critical to selecting data repositories and guarantees and
properties including functional, structure and performance.

This app measures performance on SQL tables: creating, querying, modifying
data (tuples).

# Procedure for making this app: 
Create a SQL table, calculate time to create the table (and indexes).
Allow a user to specify on a web interface:
1. A number of random queries (up to 1000 queries of random tuples in the dataset)
2. A restricted set of queries, similar to previous (1.) but where selection is
restricted (ie only occurring in CA, or within N<100 km of a specified lat,long
location.
Or: a time range, or a magnitude range.
3. Measure time expended to perform these queries.
4. Show results.
Users of this service will interact with your performance service through web page
interfaces, all processing and web service hosting is (of course) cloud based.
