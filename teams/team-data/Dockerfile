# Image du worker edge de l'équipe team-data (Python 3.12, polars + boto3).
# Versions Airflow / edge3 identiques au serveur central (sinon le worker s'arrête).
FROM apache/airflow:3.3.2-python3.12

USER airflow
# Les contraintes officielles figent airflow 3.3.2 et edge3 4.3.2 (et les versions de polars/boto3).
RUN pip install --no-cache-dir \
      "apache-airflow==3.3.2" \
      "apache-airflow-providers-edge3==4.3.2" \
      polars boto3 pytest \
      --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt

# Les DAGs de l'équipe, au chemin du bundle "team-data" (même nom de bundle qu'au centre).
COPY --chown=airflow:root dags/ /opt/airflow/dags/team-data/

# Worker edge : queue team-data, 2 slots, hostname stable (surchargé par les args k8s si besoin).
CMD ["airflow", "edge", "worker", "-q", "team-data", "-c", "2", "-H", "team-data-worker-1"]
