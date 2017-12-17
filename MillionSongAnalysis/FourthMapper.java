package MillionSongAnalysis;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// Top ten artist and title of the fast songs on the basis of their tempo

public class FourthMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] field = value.toString().split("\t");
		String tempo = field[47];
		String artistName = field[11];
		String title = field[50];
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
			double avgTempo = temp / tempoArray.length;
			finalTempo = String.valueOf(avgTempo);
			if(!isDouble(title))
			{
				context.write(new DoubleWritable(Double.parseDouble(finalTempo)), new Text(artistName + "----" + title));
			}
		}
		else if(!tempo.contains("tempo"))
		{
			context.write(new DoubleWritable(Double.parseDouble(tempo)), new Text(artistName + "----" + title));
		}
	}
	
	boolean isDouble(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

}
