-- set io.sort.mb 10;
-- customers = LOAD '/user/hadoop/data/Customers' USING PigStorage(',') AS (ID, Name, Age, Gender, CountryCode, Salary);
customers = LOAD '$input' USING PigStorage(',') AS (ID, Name, Age, Gender, CountryCode, Salary);
country_groups = GROUP customers by (CountryCode);
country_count = FOREACH country_groups GENERATE group, COUNT(customers.ID) as c;
country_count_clean = FILTER country_count BY c > 5000 or c < 2000;
countries = FOREACH country_count_clean GENERATE group;
STORE countries INTO '$output';
-- STORE countries INTO '/user/hadoop/output/pig';