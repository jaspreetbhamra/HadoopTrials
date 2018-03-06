package ted_talks;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Query2Reducer extends Reducer<Text, Text, Text, Text> {
	
//	static long arr[] = new long[2];
//	public Query2Reducer() {
//		arr[0] = arr[1] = 0;
//	}

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		context.write(new Text(), new Text("*************"+_key.toString()+"*************"));
		long count = 0;
		for (Text text : values) {
			String arr[] = text.toString().split("\t\t");
			count += Long.parseLong(arr[1]);
			context.write(new Text(), new Text(arr[0]));
		}
		context.write(new Text("Total number of viewers for this rating: "), new Text(String.valueOf(count)));
		context.write(new Text(), new Text("*************"+_key.toString()+"*************"));
		context.write(new Text(), new Text());
		context.write(new Text(), new Text());
	}

}
