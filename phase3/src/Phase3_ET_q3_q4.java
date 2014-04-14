import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

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

public class Phase3_ET_q3_q4 {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, LongWritable, Text> {
		private JSONParser parser = new JSONParser();
		private LongWritable outKey = new LongWritable();
		private Text outValue = new Text();
		private String twitterId, dateInfo, place, text;

		@Override
		public void map(LongWritable key, Text value,OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			JSONObject jsonObject;
			try {
				jsonObject = (JSONObject) parser.parse(line);
				twitterId = jsonObject.get("id").toString();
				dateInfo = jsonObject.get("created_at").toString();
				try {
					JSONObject place_structure = (JSONObject) jsonObject.get("place");
					place = place_structure.get("name").toString();
					String[] places = place.split(" ");
					String place_caocatinate = places[0];
					for (int i = 1; i < places.length; i++) {
						place_caocatinate += "+" + places[i];
					}
					place = place_caocatinate;
				} catch (NullPointerException e) {
					place = "*";
				}

				text = jsonObject.get("text").toString();

				SimpleDateFormat input_format = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
				Date converted_date = input_format.parse(dateInfo);
				long unixTime = converted_date.getTime() / 1000;
				SimpleDateFormat output_format = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
				String format_tweet_date = output_format.format(converted_date);

				String value_set = twitterId + "\t" + place + "\t" + text + "\t" + format_tweet_date;
				
				outKey.set(unixTime);
				outValue.set(value_set);
				output.collect(outKey, outValue);
				
			} catch (ParseException | java.text.ParseException e1) {
				e1.printStackTrace();
			}

		}

	}

	public static class Reduce extends MapReduceBase implements
			Reducer<LongWritable, Text, LongWritable, Text> {
		private Text outValue = new Text();

		public void reduce(LongWritable key, Iterator<Text> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				outValue.set(values.next());
				output.collect(key, outValue);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Phase3_ET_q3_q4.class);
		conf.setJobName("giraffes are cute");

		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(LongWritable.class);
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