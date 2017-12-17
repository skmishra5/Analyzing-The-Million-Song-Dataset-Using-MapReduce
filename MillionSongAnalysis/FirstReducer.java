package MillionSongAnalysis;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FirstReducer extends Reducer<Text, Text, Text, Text>{
	
	private HashMap<String, String> artistAndGenre = new HashMap<String, String>();
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		//int sum = values.toString().length();
		
		
		for (Text val: values)
		{
			//sum += val.get();
			if(!artistAndGenre.containsKey(key.toString()))
			{
				artistAndGenre.put(key.toString(), val.toString());
				context.write(key, new Text(val.toString()));
			}
		}
		
		
		
		//context.write(key, new Text(values.toString()));
		
		//context.write(new Text(String.valueOf(sum)), key);
	}

}
