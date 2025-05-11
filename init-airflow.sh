#!/bin/bash
echo '⏳ Waiting for PostgreSQL...'
until pg_isready -h postgres -p 5432 -U airflow; do
  sleep 2
done
echo '✅ PostgreSQL is ready!'
airflow db migrate
airflow db users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin