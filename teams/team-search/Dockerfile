# Worker edge de l'équipe team-search (Python 3.13 = image officielle par défaut)
FROM apache/airflow:3.3.2

USER airflow
# Versions Airflow/edge3 identiques au central ; libs spécifiques à l'équipe.
# Le client elasticsearch 9.x (épinglé 9.5.1 par les contraintes) envoie
# "compatible-with=9" -> refusé par un serveur ES 8.x (HTTP 400 media_type_header_exception).
# On retire donc ces 2 pins (elasticsearch, elastic-transport) et on force elasticsearch<9.
RUN curl -fsSL https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.13.txt \
      | grep -vE '^(elasticsearch|elastic-transport)==' > /tmp/constraints.txt \
 && pip install --no-cache-dir \
      "apache-airflow-providers-edge3==4.3.2" \
      "elasticsearch>=8.15,<9" pandas pyarrow boto3 pytest \
      --constraint /tmp/constraints.txt \
 && rm /tmp/constraints.txt

# Bundle de DAGs "team-search" (même nom et chemin que côté central)
COPY dags/ /opt/airflow/dags/team-search/

CMD ["airflow", "edge", "worker", "-q", "team-search", "-c", "2", "-H", "team-search-worker-1"]
