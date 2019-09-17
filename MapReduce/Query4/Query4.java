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

public class Query4 {

	public static class CountryMapper extends Mapper<Object, Text, Text, FloatWritable> {

		private HashMap<Integer, Integer> custCountryMap = new HashMap<Integer, Integer>();
		private HashMap<Integer, Integer> countryCustNumMap = new HashMap<Integer, Integer>();

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			URI[] uris = DistributedCache.getCacheFiles(conf);
			FSDataInputStream dataIn = FileSystem.get(conf).open(new Path(uris[0]));
			BufferedReader br = new BufferedReader(new InputStreamReader(dataIn));
			String line = br.readLine();

			while (line != null) {
				String[] tokens = line.split(",");

				int key = Integer.parseInt(tokens[0]);
				int countrycode = Integer.parseInt(tokens[4]);
				custCountryMap.put(key, countrycode);

				int count = countryCustNumMap.containsKey(countrycode) ? countryCustNumMap.get(countrycode) : 0;
				countryCustNumMap.put(countrycode, count + 1);
				
				line = br.readLine();
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");

			Text countryCodeKey = new Text();
			FloatWritable transTotal = new FloatWritable();

			int joinID = Integer.parseInt(tokens[1]);
			int countryCode = custCountryMap.get(joinID);
			int custNumber = countryCustNumMap.get(countryCode);

			countryCodeKey.set(String.format("%d,%d", countryCode, custNumber));
			transTotal.set(Float.parseFloat(tokens[2]));
			context.write(countryCodeKey, transTotal);
		}
	}

	public static class FilterRecuder extends Reducer<Text, FloatWritable, Text, Text> {
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			float max = Integer.MIN_VALUE;
			float min = Integer.MAX_VALUE;

			for (FloatWritable f : values) {
				float currTransTotal = f.get();
				if (currTransTotal > max)
					max = currTransTotal;
				if (currTransTotal < min)
					min = currTransTotal;
			}

			context.write(key, new Text(String.format("%f,%f", min, max)));
		}
	}

	public static void main(String[] args) throws Exception {

		long start = new Date().getTime();
		Configuration conf = new Configuration();
		conf.set("mapred.jop.tracker", "hdfs://localhost:8020");
		conf.set("fs.default.name", "hdfs://localhost:8020");
		DistributedCache.addCacheFile(new URI("hdfs://localhost:8020/user/hadoop/data/Customers"), conf);
		Job job = new Job(conf, "Filter");

		job.setJarByClass(Query4.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		job.setMapperClass(CountryMapper.class);
		job.setReducerClass(FilterRecuder.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		long end = new Date().getTime();
		System.out.println(String.format("Job executed in %d seconds", (end - start) / 1000));
	}

}