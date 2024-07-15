# Pyspark

## 1.Arrancar servicios

### PySpark Cluster

```
docker compose up -d spark-master spark-worker
```

### Instalar pyspark 3.5.1 en uno de los contenedores

```
docker exec -it spark-master pip3 install pyspark==3.5.1 
```

#### 1. Query Mongo

```
docker exec -it spark-master python3 /opt/spark-apps/src/mongo.py
```

#### 2. Query PostgreSQL

```
docker cp jars/postgresql-42.7.3.jar spark-master:/opt/bitnami/spark/jars
```

```
docker exec -it spark-master python3 /opt/spark-apps/src/Postgres.py
```

#### 3. Query Iceberg

```
docker exec -it spark-master python3 /opt/spark-apps/src/Write_Iceberg.py.py
```

```
docker exec -it spark-master python3 /opt/spark-apps/src/Read_Iceberg.py.py
```

#### 3. Query kafka

```
docker exec -it spark-master python3 /opt/spark-apps/src/kafka.py
```

### Iceberg

```
docker compose up -d minio
```


### kafka

```
docker compose up -d redpanda-0 console
```

### Ksqldb

```
docker compose up -d ksqldb-server ksqldb-cli
```

```
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```
