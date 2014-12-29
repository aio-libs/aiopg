-- run as "sudo -u postgres psql -ae < setup_test_db.sql" 
-- do "sudo apt-get install postgresql-contrib" first

drop database if exists aiopg;
drop user if exists aiopg;
create database aiopg;
create user aiopg with password 'passwd';
grant all privileges on database aiopg to aiopg;
\connect aiopg;
drop extension if exists hstore;
create extension hstore;
