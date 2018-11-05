# Bluebottle exercises 2018

This project should be run with Python 3 or later.  The only dependency is sqlite3, and it was developed with version 3.19.3.  The main script, etl.py, will load external CSV files into staging tables in sqlite, and eventually into tables with a smaller subset of data that is needed for the project with some small transformations (e.g. it parses the date strings in the sales file into a format that sqlite accepts).

The solutions to the exercises can be found in the sql directory, solution_1.sql and solution_2.sql (result files are available in the data directory).  There is a known issue in the loading script that inserts empty strings instead of nulls so there are a few places that check for empty strings.  This would be simple to fix in the future.  Also, the first exercise could have been more easily solved using a window function (available as of August in sqlite3 apparently), although the current implementation appears to return correct results.

I settled on using the local climatological data set from the NOAA for weather data (https://www.ncdc.noaa.gov/cdo-web/datasets#LCD).  This data set includes hourly readings for all of 2016 from a weather station at Oakland International Airport in a nicely formatted CSV file.  For this exercise, I downloaded a static file, although there is also documentation for an API that could be useful.  When I was initially looking around, I saw some promising data available at Weather Underground (https://www.wunderground.com/) although it seemed like it would be much more challenging to access an entire year's worth of historical data.
