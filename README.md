# SEEi
Developed by: Yan Jiang
Date: Feb, 2017


This is a tool that help you to search tweets faster, be more engaged with friend, and explore the reach impact. Specifically, the search tool support four features, search by:

Weblink: http://www.seei.site/

## What you can do with SEEi
Search any one that you want to stalk, by:

User ID

Keyword

Time Windwo

Social Network


## Data Source
Twitter data is stored on S3 (2015-05 to 2015-08). 
Size: 1.4 TB

## Data Pipeline
Twitter data is read from S3. Spark clean the data and did map-reduce job by a 4 node cluster. Spark cluster writes to an elasticsearch cluster which is set up on 4 node cluster. 

The front end is served with a flask app which interacts with elasticsearch clusters.
