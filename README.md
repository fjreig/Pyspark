# Pyspark

## 1.Arrancar servicios

### PySpark Cluster

```
docker compose up -d spark-master spark-worker
```


```
docker exec -it spark-master python3 /opt/spark-apps/mongo.py
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

### Instalar pyspark 3.5.1 en uno de los contenedores

```
docker exec -it spark-master pip3 install pyspark==3.5.1 
```

