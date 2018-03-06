package ted_talks;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Query3Reducer extends Reducer<Text, LongWritable, Text, Text> {

	static long max = 0;
	static String tagForMax;
	
	public void reduce(Text _key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
//		context.write(new Text(), new Text());
		long count = 0;
		for (LongWritable val : values) {
			++count;
		}
		System.out.println(_key);
		if (count > max) {
			max = count;
			tagForMax = _key.toString();
		}
		context.write(new Text("Tag: "+_key), new Text(String.valueOf(count)));
		context.write(new Text("Maximum used tag till current row: " + tagForMax), new Text("Number of occurrences of said maximum used tag: " + max));
		context.write(new Text(), new Text());
	}

}
