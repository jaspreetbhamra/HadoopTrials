package ted_talks;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Query5Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String arr[] = ivalue.toString().split("\t");
		long maxCountForRating = 0;
		long maxCount = 0;
		String maxName = "";
		try {
			JSONObject related_talks = new JSONObject("{\"data\":"+arr[11]+"}");
//			System.out.println(arr[10]);
			JSONArray array = related_talks.getJSONArray("data");
			JSONObject ratings = new JSONObject("{\"arr\":"+arr[10]+"}");
//			System.out.println(arr[10]);
			JSONArray genre_array = ratings.getJSONArray("arr");
			for(int i = 0, size = array.length(); i < size; ++i) {
				JSONObject elementOfArray = array.getJSONObject(i);
				if(elementOfArray.getLong("viewed_count")>maxCount) {
					maxCount = elementOfArray.getLong("viewed_count");
//					maxName = elementOfArray.getString("title");	//Name of the video with the maximum viewed count
				}
//				System.out.println(elementOfArray.getString("name");
				JSONObject elementOfArrayForRating = genre_array.getJSONObject(i);
				if(elementOfArrayForRating.getLong("count")>maxCountForRating) {
					maxCountForRating = elementOfArrayForRating.getLong("count");
					maxName = elementOfArrayForRating.getString("name");	//Given rating e.g. Funny, OK, etc
				}
//				Sending to the reducer: 
//					Key: Rating given by the most number of people (Name)
//					Value: Name of the video + \t + maxViewCountOfARelatedVideo + "\t" + views for the current video
				context.write(new Text(maxName), new Text(arr[7]+"\t"+String.valueOf(maxCountForRating)+"\t"+arr[16]));
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

}
