-- create the customers table
CREATE TABLE dbo.customers (
    customer_id INT NOT NULL IDENTITY PRIMARY KEY,
    first_name  NVARCHAR(128) NOT NULL,
    last_name   NVARCHAR(128) NOT NULL,
    email       NVARCHAR(128) NOT NULL,
    city        NVARCHAR(128) NOT NULL
);

-- insert a record into it
INSERT INTO dbo.customers VALUES(
    'Bruce',
    'Wayne',
    'bwayne@example.com',
    'Gotham City'
)

-- enable CDC on the database
EXEC sys.sp_cdc_enable_db;
-- enable CDC on the customers table
-- NOTE: will not indicate a change for existing data
EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name   = N'customers',
  @role_name     = NULL;

-- insert more records
INSERT INTO dbo.customers VALUES(
    'Clark',
    'Kent',
    'ckent@example.com',
    'Metropolis'
);
INSERT INTO dbo.customers VALUES(
    'Diana',
    'Prince',
    'dprince@example.com',
    'UNKNOWN'
);

-- at some point Lex Luthor has broken in...
INSERT INTO dbo.customers VALUES(
    'Lex',
    'Luthor',
    'lluthor@haxxordu.com',
    'Everywhere!!!'
);

-- and then revealed Clark's secret email
UPDATE dbo.customers
SET email = 'superman@lexwashere.com'
WHERE customer_id = 2;

-- cyborg is on the case and removes Lex's entry
DELETE FROM dbo.customers
WHERE email = 'lluthor@haxxordu.com';

-- and smoothes things over for Supes; nothing to see here
UPDATE dbo.customers
SET email = 'ckent@example.com', city = 'Smallville'
WHERE customer_id = 2;
