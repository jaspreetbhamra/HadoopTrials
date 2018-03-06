package ted_talks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		Job job1 = Job.getInstance(conf, "ted_talks");
//		job1.setJarByClass(ted_talks.Main.class);
//		job1.setMapperClass(ted_talks.Query1Mapper.class);
//		job1.setReducerClass(ted_talks.Query1Reducer.class);
//		job1.setMapOutputKeyClass(Text.class);
//		job1.setMapOutputValueClass(Text.class);
//		job1.setOutputKeyClass(Text.class);
//		job1.setOutputValueClass(Text.class);
//		FileInputFormat.addInputPath(job1, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\inputs\\ted_main.tsv"));
//		FileOutputFormat.setOutputPath(job1, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\output1"));
//		if (!job1.waitForCompletion(true))
//			return;
		
//		Job job2 = Job.getInstance(conf, "ted_talks");
//		job2.setJarByClass(ted_talks.Main.class);
//		job2.setMapperClass(ted_talks.Query2Mapper.class);
//		job2.setReducerClass(ted_talks.Query2Reducer.class);
//		job2.setMapOutputKeyClass(Text.class);
//		job2.setMapOutputValueClass(Text.class);
//		job2.setOutputKeyClass(Text.class);
//		job2.setOutputValueClass(Text.class);
//		FileInputFormat.addInputPath(job2, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\inputs\\ted_main.tsv"));
//		FileOutputFormat.setOutputPath(job2, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\output2"));
//		if (!job2.waitForCompletion(true))
//			return;	
		
//		Job job3 = Job.getInstance(conf, "ted_talks");
//		job3.setJarByClass(ted_talks.Main.class);
//		job3.setMapperClass(ted_talks.Query3Mapper.class);
//		job3.setReducerClass(ted_talks.Query3Reducer.class);
//		job3.setMapOutputKeyClass(Text.class);
//		job3.setMapOutputValueClass(LongWritable.class);
//		job3.setOutputKeyClass(Text.class);
//		job3.setOutputValueClass(Text.class);
//		FileInputFormat.addInputPath(job3, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\inputs\\ted_main.tsv"));
//		FileOutputFormat.setOutputPath(job3, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\output3"));
//		if (!job3.waitForCompletion(true))
//			return;
		
//		Job job4 = Job.getInstance(conf, "ted_talks");
//		job4.setJarByClass(ted_talks.Main.class);
//		job4.setMapperClass(ted_talks.Query4Mapper.class);
//		job4.setReducerClass(ted_talks.Query4Reducer.class);
//		job4.setMapOutputKeyClass(Text.class);
//		job4.setMapOutputValueClass(LongWritable.class);
//		job4.setOutputKeyClass(Text.class);
//		job4.setOutputValueClass(Text.class);
//		FileInputFormat.addInputPath(job4, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\inputs\\ted_main.tsv"));
//		FileOutputFormat.setOutputPath(job4, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\output4"));
//		if (!job4.waitForCompletion(true))
//			return;
		
		 Job job5 = Job.getInstance(conf, "ted_talks");
		 job5.setJarByClass(ted_talks.Main.class);
		 job5.setMapperClass(ted_talks.Query5Mapper.class);
		 job5.setReducerClass(ted_talks.Query5Reducer.class);
		 job5.setMapOutputKeyClass(Text.class);
		 job5.setMapOutputValueClass(Text.class);
		 job5.setOutputKeyClass(Text.class);
		 job5.setOutputValueClass(Text.class);
		 FileInputFormat.addInputPath(job5, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\inputs\\ted_main.tsv"));
		 FileOutputFormat.setOutputPath(job5, new Path("C:\\Users\\JaspreetKaur\\Desktop\\HadoopProject\\HadoopProjects\\output5"));
		 if (!job5.waitForCompletion(true))
		 	return;
	}

}
