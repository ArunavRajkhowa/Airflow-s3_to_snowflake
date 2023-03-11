-- making database and schema
BIG_DATA.BD1.MY_CSV_STAGEcreate DATABASE Big_Data;
create SCHEMA BD1;

-- creatinf file format
CREATE OR REPLACE FILE FORMAT mycsvformat
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;

-- creating int staging 
  CREATE OR REPLACE STAGE my_csv_stage
  FILE_FORMAT = mycsvformat;

-- inserting file to staging layer
  PUT file://C:\Users\Dell\Downloads\supermarket*.csv @my_csv_stage AUTO_COMPRESS=TRUE;

-- creating schema design for main table
CREATE or replace TABLE MYCSVTABLE (
  Invoice_Id VARCHAR(255),
  Branch VARCHAR(255),
  City VARCHAR(255),
  Customer_Type VARCHAR(255),
  Gender VARCHAR(255),
  Product_line VARCHAR(255),
  Unit_Price DECIMAL(10, 2),
  Quantity INT,
  Tax_5perc DECIMAL(10, 2),
  Total DECIMAL(10, 2),
  Date DATE,
  Time TIME,
  Payment VARCHAR(255),
  cogs DECIMAL(10, 2),
  gross_margin_percentage DECIMAL(10, 2),
  gross_income DECIMAL(10, 2),
  Rating INT
);

-- checking out contents inside the int staging
list @my_csv_stage;

--copying data from the int staging to my table
COPY INTO mycsvtable
  FROM @my_csv_stage/supermarket_sales.csv.gz
  FILE_FORMAT = (FORMAT_NAME = mycsvformat)
  ON_ERROR = 'skip_file';


-- Creating schemas for dims and facts

CREATE or replace TABLE MYFACTSTABLE (
  Invoice_Id VARCHAR(255),
  Unit_Price DECIMAL(10, 2),
  Quantity INT,
  Tax_5perc DECIMAL(10, 2),
  Total DECIMAL(10, 2),
  cogs DECIMAL(10, 2),
  gross_margin_percentage DECIMAL(10, 2),
  gross_income DECIMAL(10, 2),
  Rating INT
);


CREATE or replace TABLE MYDATETABLE (
  Invoice_Id VARCHAR(255),
  Date DATE,
  Time TIME
);


CREATE or replace TABLE MYADDRESSTABLE (
  Invoice_Id VARCHAR(255),
  City VARCHAR(255)
);

CREATE or replace TABLE MYCUSTOMERTABLE (
  Invoice_Id VARCHAR(255),
  Branch VARCHAR(255),
  Customer_Type VARCHAR(255),
  Gender VARCHAR(255)
);


CREATE or replace TABLE MYPRODUCTTABLE (
  Invoice_Id VARCHAR(255),
  Product_line VARCHAR(255)
);


-- loading data into the facts and dimensions table
  INSERT INTO MYFACTSTABLE (Invoice_Id ,  Unit_Price ,  Quantity ,  Tax_5perc ,  Total,  cogs ,  gross_margin_percentage ,  gross_income ,  Rating )
  WITH cte AS
    (SELECT Invoice_Id ,  Unit_Price ,  Quantity ,  Tax_5perc ,  Total,  cogs ,  gross_margin_percentage ,  gross_income ,  Rating 
     FROM MYCSVTABLE)
  SELECT Invoice_Id ,  Unit_Price ,  Quantity ,  Tax_5perc ,  Total,  cogs ,  gross_margin_percentage ,  gross_income ,  Rating 
  FROM cte;



  INSERT INTO MYDATETABLE (Invoice_Id,Date,Time)
  WITH cte AS
    (SELECT Invoice_Id,Date,Time
     FROM MYCSVTABLE)
  SELECT Invoice_Id,Date,Time
  FROM cte;

  INSERT INTO MYADDRESSTABLE (Invoice_Id,City)
  WITH cte AS
    (SELECT Invoice_Id,City
     FROM MYCSVTABLE)
  SELECT Invoice_Id,City
  FROM cte;



INSERT INTO MYPRODUCTTABLE (Invoice_Id,Product_line)
  WITH cte AS
    (SELECT Invoice_Id,Product_line
     FROM MYCSVTABLE)
  SELECT Invoice_Id,Product_line
  FROM cte;


  INSERT INTO MYCUSTOMERTABLE (Invoice_Id,Branch,Customer_Type,Gender)
  WITH cte AS
    (SELECT Invoice_Id,Branch,Customer_Type,Gender
     FROM MYCSVTABLE)
  SELECT Invoice_Id,Branch,Customer_Type,Gender
  FROM cte;
