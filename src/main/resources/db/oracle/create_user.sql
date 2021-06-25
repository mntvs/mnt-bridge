CREATE USER mnt_bridge IDENTIFIED BY mnt_bridge;
GRANT CONNECT TO mnt_bridge;
GRANT UNLIMITED TABLESPACE TO mnt_bridge;
GRANT CREATE ANY TABLE TO mnt_bridge;
GRANT CREATE ANY INDEX TO mnt_bridge;
GRANT CREATE ANY PROCEDURE TO mnt_bridge;
GRANT CREATE ANY SEQUENCE TO mnt_bridge;
GRANT CREATE ANY VIEW TO mnt_bridge;

GRANT ALTER ANY TABLE TO mnt_bridge;

GRANT DROP ANY PROCEDURE TO mnt_bridge;
GRANT DROP ANY VIEW TO mnt_bridge;
GRANT DROP ANY TABLE TO mnt_bridge;
grant create user to mnt_bridge;
