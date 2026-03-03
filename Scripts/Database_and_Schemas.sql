/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse'. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
*/

CREATE DATABASE DataWarehouse;


USE DataWarehouse;


-- Create Schemas
CREATE SCHEMA bronze;


CREATE SCHEMA silver;


CREATE SCHEMA gold;
