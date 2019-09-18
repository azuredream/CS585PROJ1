rm -r query2_classes
mkdir query2_classes
javac -classpath /usr/share/hadoop/hadoop-core-1.2.1.jar -d query2_classes ./Query2.java
jar -cvf ./query2.jar -C query2_classes/ .
hadoop dfs -rmr /user/hadoop/output
hadoop jar query2.jar q2.Query2 /user/hadoop/input/Transactions /user/hadoop/output
hadoop dfs -cat /user/hadoop/output/* | sort -n
