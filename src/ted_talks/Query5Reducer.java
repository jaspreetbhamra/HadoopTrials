package ted_talks;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Query5Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//		context.write(new Text(), new Text());
		long count = 0;
		for (Text val : values) {
			String arr[] = val.toString().split("\t");
			double percentage = (Double.valueOf(arr[2])/Double.valueOf(arr[1]))*100;
			context.write(new Text("Name of video: "+arr[0]), new Text("The popularity of the video with respect to the related videos is: " + String .valueOf(percentage)));
		}
//		System.out.println(_key);
		context.write(new Text("Genre: "+_key+"\t\t"), new Text("Number of views: "+String.valueOf(count)));
//		context.write(new Text(), new Text());
	}

}
