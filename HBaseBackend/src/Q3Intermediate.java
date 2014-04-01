import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Q3Intermediate {

	public static class MapQuery3 extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, LongWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			ObjectMapper mapper = new ObjectMapper();
			Map<?, ?> rootNode = mapper.readValue(line, Map.class);
			try {
				String original_userId = ((Map<?, ?>) ((Map<?, ?>) rootNode
						.get("retweeted_status")).get("user")).get("id")
						.toString();
				long userId = Long.parseLong(((Map<?, ?>) rootNode.get("user")).get("id")
						.toString());
				output.collect(new Text(original_userId), new LongWritable(userId));
			} catch (NullPointerException e) {
				// If retweeted_status doesn't exists, just skip this tweet.
			}

		}

	}

	public static class ReduceQuery3 extends MapReduceBase implements
			Reducer<Text, LongWritable, Text, Text> {

		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String userId_set;
			TreeSet<Long> userId_tree_set = new TreeSet<Long>();

			while (values.hasNext()) {
				userId_tree_set.add(values.next().get());
			}

			userId_set = "";
			Iterator<Long> it = userId_tree_set.iterator();
			while (it.hasNext()) {
				userId_set += it.next() + ";";
			}

			output.collect(key, new Text(userId_set));
		}
	}

	public static void main(String[] args) throws Exception {
		
		// Here is an entry point and name of the job
		// ** If you change the class's name. You also need to change this. **
		JobConf conf = new JobConf(Q3Intermediate.class);
		conf.setJobName("Generate intermediate format for q3");
		
		// Mapper's output will be <Text>\t<Long>
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(LongWritable.class);
		
		// Reducer's output will be <Text>\t<Text>
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// Here are our mapper and reducer functions
		conf.setMapperClass(MapQuery3.class);
		conf.setReducerClass(ReduceQuery3.class);

		// The input & output will be a plain text
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// Here are paths to input & output
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		// Start the job!
		JobClient.runJob(conf);
	}
}
