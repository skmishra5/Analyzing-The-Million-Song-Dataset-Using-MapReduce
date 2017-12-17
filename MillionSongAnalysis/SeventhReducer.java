package MillionSongAnalysis;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SeventhReducer extends Reducer<Text, Text, Text, Text>{

protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		int count = 0;
		
		for (Text val: values)
		{
			count++;
		}
		
		context.write(key, new Text(String.valueOf(count)));
	}
}
