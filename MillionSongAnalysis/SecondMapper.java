package MillionSongAnalysis;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// Average Tempo across all songs

public class SecondMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] field = value.toString().split("\t");
		
		String tempo = field[47];
		String[] tempoArray = null;
		String finalTempo = "";
		
		if(tempo.contains("["))
		{
			tempoArray = tempo.replaceAll("\\[", "").replaceAll("\\]", "").split(",");
			double temp = 0.0d;
			for (int i = 0; i < tempoArray.length; i++)
			{
				 temp += Double.parseDouble(tempoArray[i]); 
			}
			finalTempo = String.valueOf(temp);
			context.write(new Text("1"), new Text(finalTempo));
		}
		else if(!tempo.contains("tempo"))
		{
			context.write(new Text("1"), new Text(tempo));
		}
	}

}
