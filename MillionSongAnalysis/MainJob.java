package MillionSongAnalysis;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MainJob {

	public static void main(String[] args) {

		try {
		     Configuration conf = new Configuration();
		     // Give the MapRed job a name. You'll see this name in the Yarn webapp.
		     Job job = Job.getInstance(conf, "Q2/3/8/9 Answers");	
		     // Current class.
		     job.setJarByClass(MainJob.class);
		     // Mapper
		     job.setMapperClass(ThirdMapper.class);
		     // Combiner. We use the reducer as the combiner in this case.
		     //job.setCombinerClass(QuestionOneReducer.class);
		     // Reducer
		     job.setReducerClass(ThirdReducer.class);
		     //job.setNumReduceTasks(4);
		     // Outputs from the Mapper
		     job.setMapOutputKeyClass(Text.class);
		     job.setMapOutputValueClass(Text.class);
		     // Outputs from Reducer. It is sufficient to set only the following two properties
		     // if the Mapper and Reducer has same key and value types. It is set separately for
		     // elaboration.
		     job.setOutputKeyClass(Text.class);
		     job.setOutputValueClass(Text.class);
		     // path to input in HDFS
		     FileInputFormat.addInputPath(job, new Path(args[0]));
		     // path to output in HDFS
		     FileOutputFormat.setOutputPath(job, new Path(args[1]));
		     // Block until the job is completed.
		     System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
		     System.err.println(e.getMessage());
		} catch (InterruptedException e) {
		     System.err.println(e.getMessage());
		} catch (ClassNotFoundException e) {
		     System.err.println(e.getMessage());
		}

	}

}
