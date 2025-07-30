# PYTHON DEBEZIUM (ORACLE TO POSTGRES)

## PREPARE ORACLE DB

### START ORACLE DB

```bash
docker compose up -d oracle
```
### CONNECT TO ORACLE DB
<!-- https://debezium.io/blog/2022/09/30/debezium-oracle-series-part-1/ -->

```bash
docker exec -e ORACLE_SID=ORCLCDB -it oracle sqlplus sys as sysdba
# password: dbz
```

### Configure Oracle: Archive logs

```bash
ALTER SYSTEM SET db_recovery_file_dest_size = 10G;
ALTER SYSTEM SET db_recovery_file_dest = '/opt/oracle/oradata/ORCLCDB' scope=spfile;
SHUTDOWN IMMEDIATE
STARTUP MOUNT
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ARCHIVE LOG LIST;
```

### Configure Oracle: Redo logs
```bash
SELECT GROUP#, BYTES/1024/1024 SIZE_MB, STATUS FROM V$LOG ORDER BY 1;
SELECT GROUP#, MEMBER FROM V$LOGFILE ORDER BY 1, 2;
ALTER DATABASE CLEAR LOGFILE GROUP 1;
ALTER DATABASE DROP LOGFILE GROUP 1;
ALTER DATABASE ADD LOGFILE GROUP 1 ('/opt/oracle/oradata/ORCLCDB/redo01.log') size 400M REUSE;
ALTER SYSTEM SWITCH LOGFILE;

SELECT GROUP#, BYTES/1024/1024 SIZE_MB, STATUS FROM V$LOG ORDER BY 1;
```

### Configure Oracle: Supplemental Logging
```bash
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
```

### Configure Oracle: User setup
```bash
CONNECT sys/dbz@ORCLCDB as sysdba;
CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/ORCLCDB/logminer_tbs.dbf'
  SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;

CONNECT sys/dbz@ORCLPDB1 as sysdba;
CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/ORCLCDB/ORCLPDB1/logminer_tbs.dbf'
  SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;

CONNECT sys/dbz@ORCLCDB as sysdba;
CREATE USER c##dbzuser IDENTIFIED BY dbz DEFAULT TABLESPACE LOGMINER_TBS
  QUOTA UNLIMITED ON LOGMINER_TBS
  CONTAINER=ALL;

GRANT CREATE SESSION TO c##dbzuser CONTAINER=ALL;
GRANT SET CONTAINER TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE TO c##dbzuser CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY DICTIONARY TO c##dbzuser CONTAINER=ALL;
GRANT LOGMINING TO c##dbzuser CONTAINER=ALL;

GRANT CREATE TABLE TO c##dbzuser CONTAINER=ALL;
GRANT LOCK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT CREATE SEQUENCE TO c##dbzuser CONTAINER=ALL;

GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER=ALL;

GRANT SELECT ON V_$LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG_HISTORY TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_LOGS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_PARAMETERS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGFILE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVED_LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$TRANSACTION TO c##dbzuser CONTAINER=ALL;
```

### 
```bash
docker exec -it -e ORACLE_SID=ORCLPDB1 oracle sqlplus c##dbzuser@ORCLPDB1

DROP table customers;
DROP table customers1;
DROP table customers2;
CREATE TABLE customers (id number(9,0) primary key, name varchar2(50));
ALTER TABLE customers ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO customers VALUES (6021, 'Salles Thomas');
INSERT INTO customers VALUES (6022, 'George Bailey');
INSERT INTO customers VALUES (6023, 'Edward Walker');
INSERT INTO customers VALUES (6024, 'Anne Kretchmar');

CREATE TABLE customers1 (id number(9,0) primary key, name varchar2(50));
ALTER TABLE customers1 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO customers1 VALUES (2021, 'Salles Thomas');
INSERT INTO customers1 VALUES (2022, 'George Bailey');
INSERT INTO customers1 VALUES (2023, 'Edward Walker');
INSERT INTO customers1 VALUES (2024, 'Anne Kretchmar');

CREATE TABLE customers2 (id number(9,0) primary key, name varchar2(50));
ALTER TABLE customers2 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO customers2 VALUES (2021, 'Salles Thomas');
INSERT INTO customers2 VALUES (2022, 'George Bailey');
INSERT INTO customers2 VALUES (2023, 'Edward Walker');
INSERT INTO customers2 VALUES (2024, 'Anne Kretchmar');

```

## PREPARE POSTGRES DB

### START POSTGRES DB

```bash
docker compose up -d postgres
```

### CONNECT TO POSTGRES DB
```bash
docker exec -it postgres bash
psql -h postgresql -p 5432 -d postgres -U postgres
# password: postgres123AA

# postgres=# \l
#                                                        List of databases
#    Name    |  Owner   | Encoding | Locale Provider |   Collate   |    Ctype    | ICU Locale | ICU Rules |   Access privileges   
# -----------+----------+----------+-----------------+-------------+-------------+------------+-----------+-----------------------
#  postgres  | postgres | UTF8     | libc            | en_US.UTF-8 | en_US.UTF-8 |            |           | 
#  template0 | postgres | UTF8     | libc            | en_US.UTF-8 | en_US.UTF-8 |            |           | =c/postgres          +
#            |          |          |                 |             |             |            |           | postgres=CTc/postgres
#  template1 | postgres | UTF8     | libc            | en_US.UTF-8 | en_US.UTF-8 |            |           | =c/postgres          +
#            |          |          |                 |             |             |            |           | postgres=CTc/postgres
# (3 rows)

# postgres=# \dt
# Did not find any relations.
# postgres=# 
```

## START DBZ CONTAINER

```bash
rm -rf ./storage/*
docker compose up -d dbz
docker logs -f dbz
```