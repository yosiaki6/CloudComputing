import java.text.ParseException;
import java.util.Date;
import java.util.Scanner;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;


public class Q2Intermediate {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// JSON -> TreeMap
		int rowCount = 0;
		Scanner input = new Scanner(System.in);
		while (input.hasNextLine()) {
			JSONObject obj;
			try {
				obj = new JSONObject(input.nextLine());
				
				//<userId>	<outDate>	<tweetId>
				JSONObject userObj = obj.getJSONObject("user");
				String userId = (String) userObj.get("id_str");
				String strInDate = (String) obj.get("created_at");
				Date inDate;
				try {
					inDate = Constants.DATE_INPUT_FORMAT.parse(strInDate);
				} catch (ParseException e) {
					e.printStackTrace();
					continue;
				}
				String outDate = Constants.DATE_OUTPUT_FORMAT.format(inDate);
				String tweetId = (String) obj.get("id_str");

				System.out.println(userId + "\t" + outDate + "\t" + tweetId);
				
				// Print status
				rowCount++;
				if (rowCount % 10000 == 0) {
					System.err.println("["+rowCount + "] " + userId + "\t" + outDate + "\t"
							+ tweetId);
				}
			} catch (JSONException e) {
				e.printStackTrace();
				break;
			}
		}
		System.err.println("Done ["+rowCount+"]");
	}

}
