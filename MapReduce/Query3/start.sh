rm -r query3_classes
mkdir query3_classes
javac -classpath /usr/share/hadoop/hadoop-core-1.2.1.jar -d query3_classes ./Query3.java
jar -cvf ./query3.jar -C query3_classes/ .
hadoop dfs -rmr /user/hadoop/output
hadoop jar query3.jar Query3 /user/hadoop/data/Transactions /user/hadoop/output
