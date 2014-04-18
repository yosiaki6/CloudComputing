import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.lang.StringEscapeUtils;
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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Query4MR {

	private static final SimpleDateFormat INPUT_DATE_FORMAT = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
	private static final SimpleDateFormat OUTPUT_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
	private static final String NEW_LINE = "!@#NEWLINE#@!";

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private JSONParser parser = new JSONParser();
		private Text outKey = new Text();
		private Text outValue = new Text();

		@Override
		public void configure(JobConf job) {

		}

		@Override
		public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			JSONObject jsonObject;
			try {
				jsonObject = (JSONObject) parser.parse(line);
				String tweetID = jsonObject.get("id").toString();
				String tweetTime = jsonObject.get("created_at").toString();
				Date convertedDate = INPUT_DATE_FORMAT.parse(tweetTime);
				String tweetTime2 = OUTPUT_DATE_FORMAT.format(convertedDate);

				//JSONObject placeObj = (JSONObject) jsonObject.get("place");
				//String place = (placeObj == null)?"":placeObj.get("name").toString();
				//place = StringEscapeUtils.escapeCsv(place);

				String text = jsonObject.get("text").toString();
				text = StringEscapeUtils.escapeCsv(text);
				text = text.replaceAll("\n", NEW_LINE);

				outKey.set(tweetTime2 + "," + tweetID);
				outValue.set(text + "");
				output.collect(outKey, outValue);

			} catch (ParseException e1) {
				e1.printStackTrace();
			} catch (java.text.ParseException e1) {
				e1.printStackTrace();
			}

		}

	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private Text blank = new Text();
		private Text outValue = new Text();

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// Convert to UNIX timestamp
			String tokens[] = key.toString().split(",");
			String unixTweetTime = "";
			try {
				Date date = OUTPUT_DATE_FORMAT.parse(tokens[0]);
				unixTweetTime = date.getTime() + "";
			} catch (java.text.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}

			// Extract map result to sort by tweet ID
			while (values.hasNext()) {
				String value = values.next().toString().replaceAll(NEW_LINE, "\n");
				outValue.set(unixTweetTime + "," + tokens[1] + "," + value);
				output.collect(blank, outValue);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(Query4MR.class);
		conf.setJobName("giraffes are cute");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);

	}
}