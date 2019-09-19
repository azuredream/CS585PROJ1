-- set io.sort.mb 10;
customers = LOAD '$customers' USING PigStorage(',') AS (ID: int, Name:chararray,Age:int,Gender:chararray,CountryCode:int,Salary:float);
transactions = LOAD '$transactions' USING PigStorage(',') AS (TransID:int,CustID:int,TransTotal:float,TransNumItems:int,TransDesc:chararray);
joined = JOIN transactions BY CustID, customers BY ID USING 'replicated';
grouped = GROUP joined BY (ID,Name,Salary);
res = FOREACH grouped GENERATE FLATTEN(group),COUNT(joined.TransID) as NumOfTransactions,
SUM(joined.TransTotal) as TotalSum, MIN(joined.TransNumItems) as MinItems;
res= ORDER res BY ID;
STORE res INTO '$output';