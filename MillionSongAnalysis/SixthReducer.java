package MillionSongAnalysis;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SixthReducer extends Reducer<Text, Text, Text, Text>{
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		double sum = 0.0d;
		int count = 0;
		
		for (Text val: values)
		{
			sum += Double.parseDouble(val.toString());
			count++;
		}
		
		double averageLoudnessPerYear = sum/count;
		context.write(new Text(key + "---- Average Loudness Per Year: "), new Text(String.valueOf(averageLoudnessPerYear)));
		
	}
}
