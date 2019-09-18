package q1;
import java.io.*;
import java.util.Date;
import java.util.HashMap;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.net.URI;
public class Query1 {

    public static class qMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");

            
            if(Integer.parseInt(tokens[2])<=50 && Integer.parseInt(tokens[2])>=20){
            	Text Name=new Text();
            	Name.set(tokens[1]);
            	Text Age=new Text();
            	Age.set(tokens[2]);
		context.write(Name, Age);
            }
            
            
        }
    }

    public static void main(String[] args) throws Exception {

        long start = new Date().getTime();
        Configuration conf = new Configuration();
        conf.set("mapred.jop.tracker", "hdfs://localhost:8020");
        conf.set("fs.default.name", "hdfs://localhost:8020");
        DistributedCache.addCacheFile(new URI("hdfs://localhost:8020/user/hadoop/input/Customers"), conf);
        Job job = new Job(conf, "Query1");

        job.setJarByClass(Query1.class);

        job.setMapperClass(qMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        long end = new Date().getTime();
        System.out.println(String.format("Job executed in %d seconds", (end - start) / 1000));
    }

}
