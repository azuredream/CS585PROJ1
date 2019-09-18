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

public class Query3 {

    public static class TransInfoWritable implements Writable {
        private FloatWritable transTotal;
        private IntWritable transNumItems;

        public TransInfoWritable() {
            transNumItems = new IntWritable();
            transTotal = new FloatWritable();
        }

        public TransInfoWritable(int transNumItems, float transTotal) {
            this.transNumItems = new IntWritable(transNumItems);
            this.transTotal = new FloatWritable(transTotal);
        }

        public int getTransNumItems() {
            return transNumItems.get();
        }

        public Float getTransTotal() {
            return transTotal.get();
        }

        @Override
        public String toString() {
            return transNumItems.toString() + " " + transTotal.toString();
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            transNumItems.readFields(in);
            transTotal.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            transNumItems.write(out);
            transTotal.write(out);
        }
    }

    public static class CustomerMapper extends Mapper<Object, Text, Text, TransInfoWritable> {

        private HashMap<Integer, String> group_to_Id = new HashMap<Integer, String>();

        public void setup(Context context) throws IOException {
            String[] tokens;
            URI[] uris = DistributedCache.getCacheFiles(context.getConfiguration());
            FSDataInputStream dataIn = FileSystem.get(context.getConfiguration()).open(new Path(uris[0]));
            BufferedReader br = new BufferedReader(new InputStreamReader(dataIn));
            String line = br.readLine();
            while (line != null) {
                tokens = line.split(",");
                int key = Integer.parseInt(tokens[0]);
                group_to_Id.put(key, String.format("%s,%s", tokens[1], tokens[5]));
                line = br.readLine();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            
            int custID = Integer.parseInt(tokens[1]);
            float transTotal = Float.parseFloat(tokens[2]);
            int transNumItems = Integer.parseInt(tokens[3]);

            Text customerMapKey = new Text();
            customerMapKey.set(String.format("%s,%s", custID, group_to_Id.get(custID)));

            TransInfoWritable customerMapOutput = new TransInfoWritable(transNumItems, transTotal);

            context.write(customerMapKey, customerMapOutput);
        }
    }

    public static class TransReducer extends Reducer<Text, TransInfoWritable, Text, Text> {
        public void reduce(Text key, Iterable<TransInfoWritable> values, Context context)
                throws IOException, InterruptedException {
            float min = Integer.MAX_VALUE;
            float sum = 0;
            int count = 0;

            for (TransInfoWritable f : values) {
                float currTransTotal = f.getTransTotal();
                int currNumItems = f.getTransNumItems();

                if (currNumItems
                 < min)
                    min = currNumItems
                    ;
                sum += currTransTotal;
                count++;
            }
            context.write(key, new Text(String.format("%d,%f,%f", count, sum, min)));
        }
    }

    public static void main(String[] args) throws Exception {

        long start = new Date().getTime();
        Configuration conf = new Configuration();
        conf.set("mapred.jop.tracker", "hdfs://localhost:8020");
        conf.set("fs.default.name", "hdfs://localhost:8020");
        DistributedCache.addCacheFile(new URI("hdfs://localhost:8020/user/hadoop/data/Customers"), conf);
        Job job = new Job(conf, "Query3");

        job.setJarByClass(Query3.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TransInfoWritable.class);
        job.setMapperClass(CustomerMapper.class);
        job.setReducerClass(TransReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        long end = new Date().getTime();
        System.out.println(String.format("Job executed in %d seconds", (end - start) / 1000));
    }

}