import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q3ImportHBase extends Configured implements Tool {

	public static class Map extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			output.collect(key, value);
		}

	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		HTable table;
		String hbaseAddress, tableName;
		List<Put> batch = new LinkedList<Put>();

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
					HColumnDescriptor hColDesc = new HColumnDescriptor(
							Constants.FAMILY_RETWEETER_ID);
					HTableDescriptor hTableDesc = new HTableDescriptor(
							tableName);
					hTableDesc.setValue(HTableDescriptor.MAX_FILESIZE, "104857600");
					hTableDesc.addFamily(hColDesc);
					admin.createTable(hTableDesc);
					admin.close();
				}
				table = new HTable(hbaseConf, tableName);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				Text value = values.next();
				String converted = value.toString().replace(';', '\n');
				Put put = new Put(Bytes.toBytes(key.toString()));
				put.add(Constants.FAMILY_RETWEETER_ID, null,
						Bytes.toBytes(converted));
				batch.add(put);
				if (batch.size() == 100000) {
					table.put(batch);
					batch.clear();
				}
				output.collect(key, value);
			}
		}

		@Override
		public void close() throws IOException {
			table.put(batch);
			table.close();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Q3ImportHBase.class);
		conf.setJobName("Q3 import data to HBase");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.set("hbaseAddress", args[1]);
		conf.set("tableName", args[2]);

		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(
				"s3://wkanchan-bucket/phase2/output/q3import/"
						+ Constants.DATE_OUTPUT_FORMAT.format(new Date())));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner
				.run(new Configuration(), new Q3ImportHBase(), args);
		System.exit(res);
	}

}
