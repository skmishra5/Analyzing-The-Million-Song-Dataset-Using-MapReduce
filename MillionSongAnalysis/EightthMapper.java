package MillionSongAnalysis;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// Most popular terms that songs have been tagged 

public class EightthMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] field = value.toString().split("\t");
		
		String artistTerms = "";
		String artistTermsFrequency;
		
		artistTerms = field[13];
		String finalArtistTerms = "";
		
		if(artistTerms.contains("["))
		{
			finalArtistTerms = artistTerms.replaceAll("\\[", "").replaceAll("\\]", "");
		}
		
		if(artistTerms.contains("\""))
		{
			finalArtistTerms = finalArtistTerms.replaceAll("\"", "");
		}
		
		String[] artistTermsArray = finalArtistTerms.split(",");
		
		if(artistTermsArray.length > 0)
		{
			for(int i = 0; i < artistTermsArray.length; i++)
			{
				context.write(new Text(artistTermsArray[i]), new Text("1"));
			}
		}
	}

}
