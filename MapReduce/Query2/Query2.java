package q2;
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
public class Query2 {
    public static class qMapper extends Mapper<Object, Text, Text, Text> {

        private HashMap<Integer, String> Id_to_Name = new HashMap<Integer, String>();

        public void setup(Context context) throws IOException {
            String[] tokens;
            URI[] uris = DistributedCache.getCacheFiles(context.getConfiguration());
            FSDataInputStream dataIn = FileSystem.get(context.getConfiguration()).open(new Path(uris[0]));
            BufferedReader br = new BufferedReader(new InputStreamReader(dataIn));
            String line = br.readLine();
            while (line != null) {
                tokens = line.split(",");
                int key = Integer.parseInt(tokens[0]);
                Id_to_Name.put(key, String.format("%s",tokens[1]));
                line = br.readLine();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            int custID = Integer.parseInt(tokens[1]);
            Text customerMapKey = new Text();
            customerMapKey.set(String.format("%d,%s", custID, Id_to_Name.get(custID)));
            Text c_t=new Text();
            c_t.set(String.format("%d,%f", 1, Float.parseFloat(tokens[2])));
            context.write(customerMapKey, c_t);
        }
    }
    public static class qReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
        	String[] tokens = key.toString().split(",");
            float sum = 0;
            int count=0;

            for (Text f : values) {
            	String[] tkofval = key.toString().split(",");
            	int tcount=Integer.parseInt(tkofval[0]);
                float transtotal = Float.parseFloat(tkofval[1]);
                sum+=transtotal;
                count+=tcount;
            }
            Text c_t=new Text();
            c_t.set(String.format("%d,%f", count,sum));        
            context.write(key,c_t);
        }
    }

    public static void main(String[] args) throws Exception {

        long start = new Date().getTime();
        Configuration conf = new Configuration();
        conf.set("mapred.jop.tracker", "hdfs://localhost:8020");
        conf.set("fs.default.name", "hdfs://localhost:8020");
        DistributedCache.addCacheFile(new URI("hdfs://localhost:8020/user/hadoop/input/Customers"), conf);
        Job job = new Job(conf, "Query2");

        job.setJarByClass(Query2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(qMapper.class);
        job.setCombinerClass(qReducer.class);
        job.setReducerClass(qReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        long end = new Date().getTime();
        System.out.println(String.format("Job executed in %d seconds", (end - start) / 1000));
    }

}
