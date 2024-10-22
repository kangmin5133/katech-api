
DROP DATABASE if exists katech_db;
CREATE DATABASE katech_db;

DROP USER IF EXISTS 'katech_user';
CREATE USER 'katech_user'@'%' IDENTIFIED BY 'tbell0518' PASSWORD EXPIRE NEVER;

GRANT ALL PRIVILEGES ON *.* TO 'katech_user'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost';

FLUSH PRIVILEGES;

use katech_db;