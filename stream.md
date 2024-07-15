
SET 'auto.offset.reset' = 'earliest';

### 1. Crear stream

```
CREATE STREAM fv (
    Fecha STRING key,
    Inversor STRING key,
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
    value_format='json',
    key_format='json',
    timestamp='Fecha',
    timestamp_format= 'yyyy-MM-dd''T''HH:mm:ss.SSSz',
    partitions=1
);
```


### 2. Crear tabla

```
CREATE TABLE tabla1 AS SELECT FORMAT_TIMESTAMP(
  Fecha, 'yyyy-MM-dd HH:00:00') as fecha, 
  Inversor, 
  round(avg(Is1), 1) as Is1, round(avg(Is2), 1) as Is2,
  round(avg(Is3), 1) as Is3, round(avg(Is4), 1) as Is4,
  round(avg(Is5), 1) as Is5, round(avg(Is6), 1) as Is6,
  round(avg(Is7), 1) as Is7, round(avg(Is8), 1) as Is8,
  round(avg(Is9), 1) as Is9, round(avg(Is10), 1) as Is10,
  round(avg(Is11), 1) as Is11, round(avg(Is12), 1) as Is12,
  round(avg(Is13), 1) as Is13, round(avg(Is14), 1) as Is14
  FROM fv
  GROUP BY FORMAT_TIMESTAMP(Fecha, 'yyyy-MM-dd HH:00:00'), Inversor;
```
