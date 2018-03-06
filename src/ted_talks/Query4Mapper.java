package ted_talks;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Query4Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String arr[] = ivalue.toString().split("\t");
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
				context.write(new Text(maxName), new LongWritable(Long.parseLong(arr[16])));
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

}
