package ted_talks;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Query3Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String arr[] = ivalue.toString().split("\t");
		try {
			JSONObject tags = new JSONObject("{\"tags\":"+arr[13]+"}");
			JSONArray array = tags.getJSONArray("tags");
//			System.out.println(array);
			for(int i = 0, size = array.length(); i < size; ++i) {
//				System.out.println("hello");
				context.write(new Text(array.get(i).toString()), new LongWritable(1));
//				System.out.println(array.get(i));
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

}
