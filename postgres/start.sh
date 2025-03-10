#!/bin/bash

# Démarrer PostgreSQL avec le script officiel
docker-entrypoint.sh postgres &

# Attendre que PostgreSQL soit prêt
until pg_isready -U airflow; do
    echo "En attente de PostgreSQL..."
    sleep 2
done

# Exécuter les commandes SQL une fois que PostgreSQL est prêt
psql -U airflow << EOF
CREATE DATABASE mydb;
GRANT ALL PRIVILEGES ON DATABASE mydb TO airflow;
EOF

# Garder PostgreSQL en avant-plan
wait