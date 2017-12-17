package MillionSongAnalysis;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// The number of songs each artist has in the database

public class SeventhMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] field = value.toString().split("\t");
		String artistID = field[4];
		String artistName = field[11];
		
		if((!artistID.equals("artist_id")) && (!artistName.equals("artist_name")))
		{
			context.write(new Text(artistName), new Text(artistID));	
		}
	}
}
