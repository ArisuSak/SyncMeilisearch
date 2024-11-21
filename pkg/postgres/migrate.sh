#!/bin/bash

# Variables
USER="osiris"
PASS=$(kubectl get secret --namespace pylon postgresql -o jsonpath="{.data.password}" | base64 -d)
HOST="localhost"
PORT="5432"
SCRIPT_PATH=$(pwd)"/scripts/init_openfga.sql"

echo ">> starting migration"

psql postgres://$USER:$PASS@$HOST:$PORT/postgres?sslmode=disable -f $SCRIPT_PATH

echo "table migration completed successfully"
