package ted_talks;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Query1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String arr[] = ivalue.toString().split("\t");
		System.out.println(arr.length);
		context.write(new Text("views"), new Text(arr[16]));
		context.write(new Text("comments"), new Text(arr[0]));
		//otherwise ignore this row due to missing values
	}

}
