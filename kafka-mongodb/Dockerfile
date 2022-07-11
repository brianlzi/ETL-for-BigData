FROM python:latest
# COPY requirements.txt /kafka-mongodb/requirements.txt
ARG shared_workspace=kafka-mongodb

COPY . ${shared_workspace}

WORKDIR ${shared_workspace}
RUN  ls && pip install -r requirements.txt

CMD ["python", "kafka_mongo.py"]