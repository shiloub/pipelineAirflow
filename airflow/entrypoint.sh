#!/bin/bash
until pg_isready -h postgres -p 5432; do
  echo "Attente que PostgreSQL soit prêt..."
  sleep 1
done
echo "PostgreSQL est prêt!"


airflow db init

airflow users list | grep admin || airflow users create \
    --username admin \
    --password secret \
    --firstname Firstname \
    --lastname Lastname \
    --role Admin \
    --email admin@example.com

airflow scheduler &
exec airflow webserver