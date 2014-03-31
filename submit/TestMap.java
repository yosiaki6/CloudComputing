import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestMap {
	public static void main(String[] argv) {
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		ObjectMapper mapper = new ObjectMapper();
		String twitterId;
		String date;
		String userId;
		String[] dateInfo;
		String month = null;

		String line = null;
		try {
			while ((line = in.readLine()) != null) {
				try {
					Map<?, ?> rootNode = mapper.readValue(line, Map.class);
					twitterId = rootNode.get("id").toString();
					dateInfo = ((String)rootNode.get("created_at")).split(" ");
					userId = ((Map<?,?>)rootNode.get("user")).get("id").toString();
					switch(dateInfo[1]){
					case "Jan":
						month = "01";
						break;
					case "Feb":
						month = "02";
						break;
					case "Mar":
						month = "03";
						break;
					case "Apr":
						month = "04";
						break;
					case "May":
						month = "05";
						break;
					case "Jun":
						month = "06";
						break;
					case "Jul":
						month = "07";
						break;
					case "Aug":
						month = "08";
						break;
					case "Sep":
						month = "09";
						break;
					case "Oct":
						month = "10";
						break;
					case "Nov":
						month = "11";
						break;
					case "Dec":
						month = "12";
						break;
					}
					System.out.println(userId +","+dateInfo[5] + "-" + month + "-" + dateInfo[2] + "+" + dateInfo[3] + "," + twitterId);
				} catch (JsonParseException e) {
					e.printStackTrace();
				} catch (JsonMappingException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
