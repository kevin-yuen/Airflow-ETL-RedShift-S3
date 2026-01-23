#!/usr/bin/env bash
set -e

# Load .env if it exists
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# connections
airflow connections add aws_credentials --conn-uri "aws://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@"
airflow connections add redshift --conn-uri "redshift://${REDSHIFT_LOGIN}:${REDSHIFT_PASSWORD}@${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DB}"

# variables
airflow variables set s3_bucket d608-udacity-final-project
ariflow variables set s3_object_log_prefix log-data
airflow variables set s3_object_song_prefix song-data
