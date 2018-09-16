/* Con el usuario sys as sysdba se crea el tablespace, usuario a ser utilizado, con sus respectivos permisos */
CREATE TABLESPACE dwh DATAFILE 'dwh.dat' SIZE 100M ONLINE; 
CREATE USER dwh IDENTIFIED BY dwh DEFAULT TABLESPACE dwh;
GRANT UNLIMITED TABLESPACE TO dwh;
GRANT EXECUTE ON SYS.DBMS_LOCK to dwh with grant option
GRANT CREATE SESSION to dwh;
GRANT CREATE TABLE to dwh;