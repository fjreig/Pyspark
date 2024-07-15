
### 1. Crear stream

```
CREATE STREAM fv (
    Fecha varchar,
    Inversor int,
    EA DOUBLE,
    Is1 DOUBLE,
    Is2 DOUBLE,
    Is3 DOUBLE,
    Is4 DOUBLE,
    Is5 DOUBLE,
    Is6 DOUBLE,
    Is7 DOUBLE,
    Is8 DOUBLE,
    Is9 DOUBLE,
    Is10 DOUBLE,
    Is11 DOUBLE,
    Is12 DOUBLE,
    Is13 DOUBLE,
    Is14 DOUBLE
    )
  WITH (
    kafka_topic='tabla1',
    timestamp = 'Fecha',
    timestamp_format = 'yyyy-MM-dd HH:mm:ss',
    value_format='json', 
    partitions=1
);
```


### 2. Crear tabla

```
CREATE TABLE tabla1 AS
  SELECT Inversor,
        round(avg(Is1), 1) as Is1
  FROM fv
  GROUP BY Inversor
  EMIT CHANGES;
```
