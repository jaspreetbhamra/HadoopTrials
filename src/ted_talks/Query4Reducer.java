package ted_talks;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Query4Reducer extends Reducer<Text, LongWritable, Text, Text> {

	public void reduce(Text _key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
//		context.write(new Text(), new Text());
		long count = 0;
		for (LongWritable val : values) {
			count += Long.valueOf(val.toString());
		}
		System.out.println(_key);
		context.write(new Text("Genre: "+_key+"\t\t"), new Text("Number of views: "+String.valueOf(count)));
//		context.write(new Text(), new Text());
	}

}
