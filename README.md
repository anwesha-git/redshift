## Introduction
A music streaming startup, Sparkify, has grown its user base and song database and wants to move its processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

We are building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

## Project Description
In this project, we will load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables. Below is the System Architecture for AWS S3 to Redshift ETL
 ![image](https://github.com/anwesha-git/redshift/assets/122990634/4db14106-a371-411d-9607-ac5cc918c0cc)

## Project Datasets
We'll be working with 3 datasets that reside in S3. Here are the S3 links for each:

### Song dataset: s3://udacity-dend/song_data
  The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.
  eg: s3.ObjectSummary(bucket_name='udacity-dend', key='song-data/A/A/C/TRAACMJ128F930C704.json')
      s3.ObjectSummary(bucket_name='udacity-dend', key='song-data/A/B/B/TRABBDN12903D0571D.json')
      s3.ObjectSummary(bucket_name='udacity-dend', key='song-data/K/J/Y/TRKJYBX128F426EF4C.json')
And below is an example of what a single song file, TRAACMJ128F930C704.json, looks like

```{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0} ```

### Log dataset: s3://udacity-dend/log_data
  The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings. The log files in the dataset are partitioned by year and month. 
  eg: s3.ObjectSummary(bucket_name='udacity-dend', key='log-data/2018/11/2018-11-01-events.json')
      s3.ObjectSummary(bucket_name='udacity-dend', key='log-data/2018/11/2018-11-02-events.json')
      s3.ObjectSummary(bucket_name='udacity-dend', key='log-data/2018/11/2018-11-03-events.json')
      
And below is an example of what the data in a log file, 2018-11-12-events.json, looks like
      ![image](https://github.com/anwesha-git/redshift/assets/122990634/300ba991-0a72-4690-8354-f865b20cd47e)

This third file s3://udacity-dend/log_json_path.json contains the meta information that is required by AWS to correctly load s3://udacity-dend/log_data. And below is what data is in log_json_path.json.

![image](https://github.com/anwesha-git/redshift/assets/122990634/f5c1fadf-0621-4c41-bd3d-42d11c1f260f)


## Schema for Song Play Analysis
Using the song and event datasets, we'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

### Fact Table
####  songplays - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
###  Dimension Tables
####  users - users in the app
user_id, first_name, last_name, gender, level
####  songs - songs in music database
song_id, title, artist_id, year, duration
####  artists - artists in music database
artist_id, name, location, lattitude, longitude
####  time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

## Project Template
The project template includes:

### create_table.py 
  This is where we'll create our fact and dimension tables for the star schema in Redshift.
### etl.py 
   This is where we'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
### sql_queries.py 
   This is where we'll define our SQL statements, which will be imported into the two other files above.
### redshift_cluster.ipynb 
   This is where we'll write code to create a redshift cluster and the required policy & role to access an S3 bucket and open the TCP port to communicate to the cluster. Finally, this notebook can be referred to decommission the cluster and roles created.
### ER.jpeg
   This contains the ER diagram for the fact-dimension table.

## Analysis
After completion of the ETL pipeline, we can do multiple analyses based on the table data. A few examples are as below:
1. most popular location where songs are played

   ``` SELECT location from (SELECT location, COUNT(*) AS play_count FROM songplays GROUP BY location ORDER BY play_count DESC LIMIT 1) ```
2. When is the highest usage time of day by the hour for songs

    ``` SELECT EXTRACT(hour FROM start_time) AS hour_of_day, COUNT(*) AS play_count FROM songplays GROUP BY hour_of_day ORDER BY play_count DESC LIMIT 1; ```
3. Find the average duration of songs in the database.

    ``` SELECT AVG(duration) AS average_duration FROM songs ```
4. Find the total duration of songs played by each artist.

     ``` SELECT artists.name AS artist_name, SUM(songs.duration) AS total_duration FROM artists JOIN songs ON artists.artist_id = songs.artist_id JOIN songplays ON songs.song_id = songplays.song_id GROUP BY artists.name; ```
