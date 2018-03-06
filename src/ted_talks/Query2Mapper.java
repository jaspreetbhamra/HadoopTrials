package ted_talks;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
//import org.json.simple.JSONObject;
//import org.json.simple.JSONValue;
//
//import com.google.gson.Gson;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonObject;
//import com.google.gson.JsonParser;

public class Query2Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String arr[] = ivalue.toString().split("\t");
//		System.out.println(arr[10]);
//		arr[10] -> ratings
//		JsonParser jsonParser = new JsonParser();
//		JsonElement r = jsonParser.parse(arr[10]);
//		JsonObject ratings = new JsonObject();
//		String jsonString = "{"+arr[10]+"}";
//		Gson gson = new Gson();
//		Ratings ratings = gson.fromJson(arr[10], Ratings.class);
//		System.out.println(ratings.getCount());
		//JSONObject ratings = (JSONObject) JSONValue.parse(arr[10]);
		long maxCount = 0;
		String maxName = null;
		try {
			JSONObject ratings = new JSONObject("{\"arr\":"+arr[10]+"}");
//			System.out.println(arr[10]);
			JSONArray array = ratings.getJSONArray("arr");
			for(int i = 0, size = array.length(); i < size; ++i) {
				JSONObject elementOfArray = array.getJSONObject(i);
				if(elementOfArray.getLong("count")>maxCount) {
					maxCount = elementOfArray.getLong("count");
					maxName = elementOfArray.getString("name");	//Given rating e.g. Funny, OK, etc
				}
//				System.out.println(elementOfArray.getString("name"));
			}
			context.write(new Text(maxName), new Text(arr[7]+"\t\t"+String.valueOf(maxCount)));
		} catch (JSONException e) {
			e.printStackTrace();
		}
		//String something = ratings.toJSONString();
//		String something = (String) ratings.get("name");
//		//System.out.println(something);
//		context.write(new Text(), new Text(arr[16]));
//		context.write(new Text("comments"), new Text(arr[0]));
	}

}
