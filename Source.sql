CREATE DATABASE test_assessment_db;

CREATE LOGIN ETL WITH PASSWORD = '2024';

USE [test_assessment_db];

CREATE USER ETL FOR LOGIN ETL;

ALTER ROLE db_owner ADD MEMBER ETL;

CREATE TABLE DataFromCSV (
    tpep_pickup_datetime DATETIME,      
    tpep_dropoff_datetime DATETIME,     
    passenger_count INT,            
    trip_distance FLOAT,                
    store_and_fwd_flag CHAR(3),         
    PULocationID INT,                   
    DOLocationID INT,                   
    fare_amount DECIMAL(10, 2),         
    tip_amount DECIMAL(10, 2)           
);
