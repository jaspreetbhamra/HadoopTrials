package ted_talks;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Query1Reducer extends Reducer<Text, Text, Text, Text> {
	
	static long arr[] = new long[2];
	public Query1Reducer() {
		arr[0] = arr[1] = 0;
	}

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		long count = 0;
		for (Text val : values) {
			long temp = Long.parseLong(val.toString());
			count += temp;
		}
		if (_key.toString().equals("comments")) 
			arr[0] = count;
		else
			arr[1] = count;
		if(arr[0]!=0 && arr[1]!=0) {
			double commentsToViews = ((double)arr[0])/((double)arr[1]);
			commentsToViews *= 100;
			context.write(new Text("The percentage of the number of people who commented on the talks as compared to those who only viewed them is "), new Text(String.valueOf(commentsToViews)));
			// System.out.println("The ratio of the number of people who commented on the talks as compared to those who only viewed them is " + commentsToViews);
		}
	}

}
