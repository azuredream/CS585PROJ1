-- set io.sort.mb 10;
customers = LOAD '$customers' USING PigStorage(',') AS (ID: int, Name:chararray, Age:int, Gender:chararray, CountryCode:int, Salary:float);
transactions = LOAD '$transactions' USING PigStorage(',') AS (TransID:int, CustID:int, TransTotal:float, TransNumItems:int, TransDesc:chararray);
customers_range = FOREACH customers GENERATE *, (CASE Age / 10
                                                WHEN 1 THEN '[10-20)' 
                                                WHEN 2 THEN '[20-30)' 
                                                WHEN 3 THEN '[30-40)' 
                                                WHEN 4 THEN '[40-50)' 
                                                WHEN 5 THEN '[50-60)' 
                                                ELSE '[60-70)' END) AS AgeRange;
cusTrans = JOIN transactions BY CustID, customers_range BY ID USING 'replicated';
cusTransByGroup = GROUP cusTrans BY (AgeRange, Gender);
cusTransInfo = FOREACH cusTransByGroup GENERATE group, MIN(cusTrans.transactions::TransTotal), MAX(cusTrans.transactions::TransTotal), AVG(cusTrans.transactions::TransTotal);
STORE cusTransInfo INTO '$output';