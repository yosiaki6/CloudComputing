import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.util.json.JSONObject;

public class MyHBase {

	private HTable htable;

	private static byte[] BYTES_CREATED_AT = Bytes.toBytes("created_at"); 
	private static byte[] BYTES_USER = Bytes.toBytes("user"); 
	private static byte[] BYTES_ID = Bytes.toBytes("id"); 

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws Exception {
		new MyHBase();
	}

	public MyHBase() throws Exception {


		Configuration conf = HBaseConfiguration.create();
		htable = new HTable(conf, "tweets");

		// Sometimes, you won't know the row you're looking for. In this case, you
		// use a Scanner. This will give you cursor-like interface to the contents
		// of the table.  To set up a Scanner, do like you did above making a Put
		// and a Get, create a Scan.  Adorn it with column names, etc.
		Scan s = new Scan();
		FilterList filter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		filter.addFilter(new SingleColumnValueFilter(
				BYTES_CREATED_AT, null, CompareOp.EQUAL, Bytes.toBytes("2014/04/04")
				));
		filter.addFilter(new SingleColumnValueFilter(
				BYTES_USER, BYTES_ID, CompareOp.EQUAL, Bytes.toBytes("9001")
				));
		s.setFilter(filter);
		//	    s.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"));
		ResultScanner scanner = htable.getScanner(s);
		try {
			// Scanners return Result instances.
			// Now, for the actual iteration. One way is to use a while loop like so:
			for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
				// print out the row we found and the columns we were looking for
				System.out.println("Found row: " +
						"created_at\t" + new String(rr.getValue(BYTES_CREATED_AT, null)) + 
						"\tuser:id\t" + new String(rr.getValue(BYTES_USER, BYTES_ID)));
			}
		} finally {
			// Make sure you close your scanners when you are done!
			// Thats why we have it inside a try/finally clause
			scanner.close();
		}
	}

}
