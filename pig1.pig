c=LOAD 'Customers' USING PigStorage"(',') AS (ID: int, Name:chararray,Age:int,Gender:chararray,CountryCode:int,Salary:float)

t = LOAD 'Transactions' USING PigStorage(',') AS (TransID:int,CustID:int,TransTotal:float,TransNumItems:int,TransDesc:chararray);



jt=JOIN t BY custID, c BY ID;



custs = FILTER jt BY MIN(TransNumItems);

result=FOREACH custs GENERATE Name,TransNumItems;


DESCRIBE result;