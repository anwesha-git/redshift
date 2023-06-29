# redshift
## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

We are building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

## Project Description
In this project, we will load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables. Below is the System Architecture for AWS S3 to Redshift ETL:

[image](https://github.com/anwesha-git/redshift/assets/122990634/4db14106-a371-411d-9607-ac5cc918c0cc)

