package MillionSongAnalysis;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SecondReducer extends Reducer<Text, Text, Text, Text>{
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		double sum = 0.0d;
		int count = 0;
		
		for (Text val: values)
		{
			sum += Double.parseDouble(val.toString());
			count++;
		}
		
		double averageTempo = sum/count;
		context.write(new Text("Average Tempo: "), new Text(String.valueOf(averageTempo)));
		
	}

}
