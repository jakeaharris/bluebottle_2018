# Extra Credit

How would productize both reports?

I think these reports could be easily productionized using a job scheduler, like Jenkins or Airflow, after working through accessing the store and weather data programmatically.  Both data sets could be staged in a data warehouse, and the store data in particular could benefit from partitioning (maybe by store ID or region) to speed up reporting.  The presentation layer could be something simple, like an API serving report data to a Django app from a fact table, or something more complex and dynamic powered by something like ElasticSearch.  This should be tailored to the consumer of the reports.

What are some tradeoffs and assumptions for your design of this ETL?

This ETL was designed without many considerations for error handling and scaling, and no tests.  While implementing the solutions, I was running with a month's worth of data and eyeballing that the results looked correct.  If this were to be developed further, adding a few tests would be a great next step.  I would also like to break up the weather table into separate tables for hourly readings and daily averages.  Adding indexes would also be high up on the list of next steps to help with scaling.  There is some basic validation for the number of expected values in the rows of each file.  I created a reusable function to read each CSV and insert into tables.  I opted to read line-by-line rather than use sqlite's import functionality since the data is relatively small, and this also made it possible to add the source file name to each row along with the current datetime for debugging.  Ideally, the configuration would be stored separately from the source code.  As for error handling, it would be nice to spend time thinking about what to do with lines that cannot be loaded, and how to handle exceptions raised midway through loading (e.g. rollback or catch and continue).

What some of the tools you would consider to build this into an ETL pipeline?

As I mentioned earlier, the ongoing ETL pipeline could be handled well by Jenkins or Airflow.  I think Python and most connectors for relational databases can handle most "medium data" scenarios, although it could be useful to use sqoop to load files if they get substantially larger, and something like Hive or Map/Reduce for processing.
