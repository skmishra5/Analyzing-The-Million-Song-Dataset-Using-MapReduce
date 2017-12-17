package MillionSongAnalysis;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// On a per-year basis, the mean variance of loudness across all songs

public class SixthMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] field = value.toString().split("\t");
		String loudness = field[27];
		String year = field[53];
		
		if(isDouble(year))
		{
			if(Double.parseDouble(year) > 0)
			{
				context.write(new Text(year), new Text(loudness));
			}
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
