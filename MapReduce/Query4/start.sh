rm -r query4_classes
mkdir query4_classes
javac -classpath /usr/share/hadoop/hadoop-core-1.2.1.jar -d query4_classes ./Query4.java
jar -cvf ./query4.jar -C query4_classes/ .
hadoop dfs -rmr /user/hadoop/output
hadoop jar query4.jar Query4 /user/hadoop/data/Transactions /user/hadoop/output
hadoop dfs -cat /user/hadoop/output/* | sort -n