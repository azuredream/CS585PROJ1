javac DataGenerator.java
java DataGenerator Customers Transactions
hadoop dfs -mkdir /user/hadoop/data2
hadoop dfs -rm /user/hadoop/data2/*
hadoop dfs -copyFromLocal Customers /user/hadoop/data2
hadoop dfs -copyFromLocal Transactions /user/hadoop/data2