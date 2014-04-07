import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.platform.Verticle;

public class Server extends Verticle {

	static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	static final String TEAM_NAME = "GiraffeLovers,5148-7320-2582\n";
	Configuration hbaseConf;
	HTable q2table, q3table;
	
	public void start() {
		String hbaseAddress = "localhost";
		hbaseConf = HBaseConfiguration.create();
		hbaseConf.set("hbase.zookeeper.quorum", hbaseAddress);
		try {
			q2table = new HTable(hbaseConf, "q2phase2");
			q3table = new HTable(hbaseConf, "q3phase2");
		} catch (IOException e1) {
			e1.printStackTrace();
			System.exit(1);
		}
		
		vertx.createHttpServer().requestHandler(new Handler<HttpServerRequest>() {

			private String handleQuery1(HttpServerRequest req) {
				String result = TEAM_NAME;
				result += DATE_FORMAT.format(new Date())+"\n";
				return result;
			}
	
			private String handleQuery2(HttpServerRequest req) {
				String result = TEAM_NAME;
				
				try {
					String tweetTime = req.params().get("tweet_time");
					tweetTime = tweetTime.replace(' ', '+');
					String userId = req.params().get("userid");
					String rowKey = tweetTime + "|" + userId;
					Get get = new Get(Bytes.toBytes(rowKey));
					get.addFamily(Bytes.toBytes("tweet_id"));
					Result r = q2table.get(get);
					if (!r.isEmpty()) {
						byte[] rawResult = r.getValue(Bytes.toBytes("tweet_id"), null);
						result += new String(rawResult);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				return result;
			}
	
			private String handleQuery3(HttpServerRequest req) {
				String result = TEAM_NAME;
				
				try {
					String userId = req.params().get("userid");
					Get get = new Get(Bytes.toBytes(userId));
					get.addFamily(Bytes.toBytes("retweeter_id"));
					Result r = q3table.get(get);
					if (!r.isEmpty()) {
						byte[] rawResult = r.getValue(Bytes.toBytes("retweeter_id"), null);
						result += new String(rawResult);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				return result;
			}
	
			public void handle(HttpServerRequest req) {
				String body = "", path = req.path();
				if (path.equals("/q1")) {
					body = handleQuery1(req);
				} else if (path.equals("/q2")) {
					body = handleQuery2(req);
				} else if (path.equals("/q3")) {
					body = handleQuery3(req);
				}
				req.response().putHeader("Content-Type", "text/plain");
				req.response().putHeader("Content-Length",
						"" + body.length());
				req.response().write(body).end();
			}
		}).listen(80);
	}
	
	@Override
	public void stop() {
		try {
			q2table.close();
			q3table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
