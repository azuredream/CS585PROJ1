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

public class Query5 {

    public static class AgeGenderMapper extends Mapper<Object, Text, Text, FloatWritable> {

        private HashMap<Integer, String> group_to_Id = new HashMap<Integer, String>();

        public static String getAgeRange(String age) {

            int rangeCode = Integer.parseInt(age) / 10;
            String output;
            switch (rangeCode) {
                case 1:
                    output = "[10-20)";
                    break;
                case 2:
                    output = "[20-30)";
                    break;
                case 3:
                    output = "[30-40)";
                    break;
                case 4:
                    output = "[40-50)";
                    break;
                case 5:
                    output = "[50-60)";
                    break;
                case 6:
                    output = "[60-70)";
                    break;
                
                default:
                    output = "[60-70)";
                    break;
            }
            return output;
        }

        public void setup(Context context) throws IOException {
            String[] tokens;
            Configuration conf = context.getConfiguration();
            URI[] uris = DistributedCache.getCacheFiles(conf);
            FSDataInputStream dataIn = FileSystem.get(context.getConfiguration()).open(new Path(uris[0]));
            BufferedReader br = new BufferedReader(new InputStreamReader(dataIn));
            String line = br.readLine();
            while (line != null) {
                tokens = line.split(",");
                int key = Integer.parseInt(tokens[0]);
                group_to_Id.put(key, String.format("%s,%s", AgeGenderMapper.getAgeRange(tokens[2]), tokens[3]));
                line = br.readLine();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            int custID = Integer.parseInt(tokens[1]);
            String[] keyTokens = group_to_Id.get(custID).split(",");
            FloatWritable TransTotal = new FloatWritable();
            Text ageGenderKey = new Text();
            TransTotal.set(Float.parseFloat(tokens[2]));
            ageGenderKey.set(group_to_Id.get(custID));
            context.write(ageGenderKey, TransTotal);
        }
    }

    public static class TransReducer extends Reducer<Text, FloatWritable, Text, Text> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float max = Integer.MIN_VALUE;
            float min = Integer.MAX_VALUE;
            float sum = 0;
            int count = 0;

            for (FloatWritable f : values) {
                float currentTrans = f.get();
                if (currentTrans > max)
                    max = currentTrans;
                if (currentTrans < min)
                    min = currentTrans;
                sum += currentTrans;
                count++;
            }
            context.write(key, new Text(String.format("%f,%f,%f", min, max, sum / count)));
        }
    }

    public static void main(String[] args) throws Exception {

        long start = new Date().getTime();
        Configuration conf = new Configuration();
        conf.set("mapred.jop.tracker", "hdfs://localhost:8020");
        conf.set("fs.default.name", "hdfs://localhost:8020");
        DistributedCache.addCacheFile(new URI("hdfs://localhost:8020/user/hadoop/data/Customers"), conf);
        Job job = new Job(conf, "Filter");

        job.setJarByClass(Query5.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setMapperClass(AgeGenderMapper.class);
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