rm -r query1_classes
mkdir query1_classes
javac -classpath /usr/share/hadoop/hadoop-core-1.2.1.jar -d query1_classes ./Query1.java
jar -cvf ./query1.jar -C query1_classes/ .
hadoop dfs -rmr /user/hadoop/output
hadoop jar query1.jar q1.Query1 /user/hadoop/input/Customers /user/hadoop/output
hadoop dfs -cat /user/hadoop/output/* | sort -n
