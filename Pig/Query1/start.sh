#! /bin/bash
hadoop dfs -rmr /user/hadoop/output
pig -f script.pig -param customers=/user/hadoop/data/Customers -param transactions=/user/hadoop/data/Transactions -param output=/user/hadoop/output
hadoop dfs -cat /user/hadoop/output/*