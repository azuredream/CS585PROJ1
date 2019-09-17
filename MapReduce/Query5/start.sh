rm -r query5_classes
mkdir query5_classes
javac -classpath /usr/share/hadoop/hadoop-core-1.2.1.jar -d query5_classes ./Query5.java
jar -cvf ./query5.jar -C query5_classes/ .
hadoop dfs -rmr /user/hadoop/output
hadoop jar query5.jar Query5 /user/hadoop/data/Transactions /user/hadoop/output
