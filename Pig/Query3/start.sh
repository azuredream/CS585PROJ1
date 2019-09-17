#! /bin/bash
hadoop dfs -rmr /user/hadoop/output
pig -f script.pig -param input=/user/hadoop/data/Customers -param output=/user/hadoop/output
hadoop dfs -cat /user/hadoop/output/*