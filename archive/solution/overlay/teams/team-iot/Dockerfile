# Image du worker edge de l'équipe team-iot (Python 3.10, numpy + scikit-learn), docs/09.
# Versions Airflow / edge3 identiques au serveur central (sinon HTTP 400, le worker s'arrête).
# Python 3.10 : choix pédagogique (edge3 5.0.0 l'abandonne, cf. docs/09 étape 13.8).
FROM apache/airflow:3.3.2-python3.10

USER airflow
# Contraintes officielles : figent airflow 3.3.2, edge3 4.3.2 et les versions des libs.
# amazon[s3fs] : ObjectStoragePath("s3://...") et backend XCom object storage (étapes 4 et 12).
RUN pip install --no-cache-dir \
      "apache-airflow==3.3.2" \
      "apache-airflow-providers-edge3==4.3.2" \
      "apache-airflow-providers-amazon[s3fs]" \
      numpy scikit-learn boto3 pytest \
      --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.10.txt

# Les DAGs de l'équipe, au chemin du bundle "team-iot" (même nom de bundle qu'au centre).
COPY --chown=airflow:root dags/ /opt/airflow/dags/team-iot/

# Worker edge : queue team-iot, 2 slots, hostname stable (surchargé par les args k8s).
CMD ["airflow", "edge", "worker", "-q", "team-iot", "-c", "2", "-H", "team-iot-worker-1"]
