set hivevar:path_hdfs_db=/spotify_playlist/DB;

DROP TABLE IF EXISTS tracks_tmp;

CREATE TABLE IF NOT EXISTS tracks_tmp (
PLAYLIST_ID string,
TRACK_ID string,
ARTIST_ID string,
DATE_TRACK date
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION 'hdfs:${path_hdfs_db}/tmp/tracks/';

DROP TABLE IF EXISTS tracks;

CREATE EXTERNAL TABLE IF NOT EXISTS tracks (
PLAYLIST_ID string,
TRACK_ID string,
ARTIST_ID string,
DATE_TRACK date
)
partitioned by(DATE_JOUR date)
STORED AS PARQUET
LOCATION 'hdfs:${path_hdfs_db}/tracks';

DROP TABLE IF EXISTS artists_tmp;

CREATE TABLE IF NOT EXISTS artists_tmp (
NAME string,
ARTIST_ID string,
DATE_ARTIST date,
POPULARITY int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION 'hdfs:${path_hdfs_db}/tmp/artists';

DROP TABLE IF EXISTS artists;

CREATE EXTERNAL TABLE IF NOT EXISTS artists (
NAME string,
ARTIST_ID string,
DATE_ARTIST date,
POPULARITY int
)
partitioned by(DATE_JOUR date)
STORED AS PARQUET
LOCATION 'hdfs:${path_hdfs_db}/artists';

DROP TABLE IF EXISTS inout_tmp;

CREATE TABLE IF NOT EXISTS inout_tmp (
PLAYLIST_ID string,
ARTIST_ID string,
DATE_INOUT date,
STATUS int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION 'hdfs:${path_hdfs_db}/tmp/inout';

DROP TABLE IF EXISTS inout;

CREATE EXTERNAL TABLE IF NOT EXISTS inout (
PLAYLIST_ID string,
ARTIST_ID string,
DATE_INOUT date,
STATUS int
)
partitioned by(DATE_JOUR date)
STORED AS PARQUET
LOCATION 'hdfs:${path_hdfs_db}/inout';
