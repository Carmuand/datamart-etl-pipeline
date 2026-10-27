#!/usr/bin/env bash
# Inicialización automática de Airflow: migración, admin, Connections y Variables.
# Es idempotente: se puede ejecutar varias veces sin romper nada.
#!/bin/bash
echo "Inicializando base de datos de metadatos de Airflow..."
airflow db upgrade

echo "Creando usuario administrador por defecto..."
airflow users create \
    --username admin \
    --firstname Data \
    --lastname Engineer \
    --role Admin \
    --email admin@datamart.com \
    --password admin

echo "Inicialización completada con éxito. Arrancando Scheduler..."
exec airflow scheduler
