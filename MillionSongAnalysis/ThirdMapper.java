package MillionSongAnalysis;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// Median danceability score across all songs

public class ThirdMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] field = value.toString().split("\t");
		
		String danceability = field[21];
		String artistName = field[11];
		String finalDanceability = "";
		String[] daceabilityArray = null;
		String title = field[50];
		
		if(danceability.contains("["))
		{
			daceabilityArray = danceability.replaceAll("\\[", "").replaceAll("\\]", "").split(",");
			double temp = 0.0d;
			for (int i = 0; i < daceabilityArray.length; i++)
			{
				 temp += Double.parseDouble(daceabilityArray[i]); 
			}
			double avgDanceability = temp / daceabilityArray.length;
			finalDanceability = String.valueOf(avgDanceability);
			context.write(new Text("one"), new Text(finalDanceability));
		}
		else if(!danceability.contains("danceability"))
		{
			context.write(new Text("one"), new Text(danceability));
			//context.write(new Text("1"), new Text(danceability));
		}
	}

}
