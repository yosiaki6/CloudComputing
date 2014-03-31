import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CSV2HBase extends Configured implements Tool {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private int count = 0;
		private Text rowKey = new Text();
		private Text tweetIDs = new Text();
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String[] token = value.toString().split(",");
			rowKey.set(token[0]+","+token[1]);
			tweetIDs.set(token[2]);
			output.collect(rowKey, tweetIDs);
			reporter.setStatus("Finished mapping " + ++count);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private int count = 0;
		private HTable table;
		private String hbaseAddress,tableName;
		
		@Override
		public void configure(JobConf job) {
			hbaseAddress = job.get("hbaseAddress");
			tableName = job.get("tableName");
			
			Configuration hbaseConf = HBaseConfiguration.create();
			hbaseConf.set("hbase.zookeeper.quorum", hbaseAddress);
			HBaseAdmin admin;
			try {
				admin = new HBaseAdmin(hbaseConf);
				if (!admin.tableExists(tableName)) {
					HColumnDescriptor hColDesc = new HColumnDescriptor(Constants.FAMILY_TWEET_ID);
					HTableDescriptor hTableDesc = new HTableDescriptor(tableName);
					hTableDesc.addFamily(hColDesc);
					admin.createTable(hTableDesc);
					admin.close();
				}
				table = new HTable(hbaseConf, tableName);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			while (values.hasNext()) {
				String original = values.next().toString();
				String converted = original.replace(';', '\n');
				Put put = new Put(key.getBytes());
				put.add(Constants.FAMILY_TWEET_ID, null, Bytes.toBytes(converted));
				table.put(put);
				output.collect(key, new Text(original));
			}
			reporter.setStatus("Finished reducing " + ++count);
		}
		
		@Override
		public void close() throws IOException {
			table.close();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), CSV2HBase.class);
		conf.setJobName("importhbase");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.set("hbaseAddress", args[1]);
		conf.set("tableName", args[2]);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path("s3://wkanchan-bucket/phase2/output/"+
				Constants.DATE_OUTPUT_FORMAT.format(new Date())));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {	
		int res = ToolRunner.run(new Configuration(), new CSV2HBase(), args);
		System.exit(res);
	}

}
