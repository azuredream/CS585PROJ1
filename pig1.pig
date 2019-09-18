c = LOAD '/user/hadoop/input/Customers' USING PigStorage(',') AS (ID: int, Name:chararray,Age:int,Gender:chararray,CountryCode:int,Salary:float);
t = LOAD '/user/hadoop/input/Transactions' USING PigStorage(',') AS (TransID:int,CustID:int,TransTotal:float,TransNumItems:int,TransDesc:chararray);
jointable = JOIN t BY CustID, c BY ID USING 'replicated';
group_name = GROUP jointable BY Name;
name_trans = FOREACH group_name GENERATE group,COUNT(jointable.CustID) as transcount;
name_trans_ord= ORDER name_trans BY transcount;
minline = LIMIT name_trans_ord 1;
res = FILTER name_trans BY transcount==minline.transcount;
DUMP res;
