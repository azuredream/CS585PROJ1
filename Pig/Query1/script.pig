-- set io.sort.mb 10;
customers = LOAD '$customers' USING PigStorage(',') AS (ID: int, Name:chararray, Age:int, Gender:chararray, CountryCode:int, Salary:float);
transactions = LOAD '$transactions' USING PigStorage(',') AS (TransID:int, CustID:int, TransTotal:float, TransNumItems:int, TransDesc:chararray);
cusTrans = JOIN transactions BY CustID, customers BY ID USING 'replicated';
cusTransByGroup = GROUP cusTrans BY (CustID, Name);
numTransByCust = FOREACH cusTransByGroup GENERATE group, COUNT(cusTrans.transactions::TransID) AS numTrans;
numTransByCustOrd = GROUP numTransByCust ALL;
minNumTrans = FOREACH numTransByCustOrd GENERATE MIN(numTransByCust.numTrans) AS minVal;
custWithMintrans = FILTER numTransByCust BY numTrans == (long)minNumTrans.minVal;
STORE custWithMintrans INTO '$output';