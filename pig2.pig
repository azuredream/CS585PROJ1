c = LOAD '/user/hadoop/input/Customers' USING PigStorage(',') AS (ID: int, Name:chararray,Age:int,Gender:chararray,CountryCode:int,Salary:float);
t = LOAD '/user/hadoop/input/Transactions' USING PigStorage(',') AS (TransID:int,CustID:int,TransTotal:float,TransNumItems:int,TransDesc:chararray);
jt = JOIN t BY CustID, c BY ID USING 'replicated';
groupcc = GROUP jt BY (ID,Name,Salary);
res = FOREACH groupcc GENERATE FLATTEN(group),COUNT(jt.TransID) as NumOfTransactions,
SUM(jt.TransTotal) as TotalSum, MIN(jt.TransNumItems) as MinItems;
res= ORDER res BY ID;
DUMP res;
