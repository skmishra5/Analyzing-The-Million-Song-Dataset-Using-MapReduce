package MillionSongAnalysis;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// The most commonly tagged genre for each artist

public class FirstMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	//private final static IntWritable one = new IntWritable(1);
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] field = value.toString().split("\t");
		
		String artistTerms = "";
		String artistTermsFrequency;
		String artistName = "";
		
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
			artistTermsFrequency = field[14];
			
			String finalArtistTermsFreq = "";
			
			if(artistTermsFrequency.contains("["))
			{
				finalArtistTermsFreq = artistTermsFrequency.replaceAll("\\[", "").replaceAll("\\]", "");
			}
			
			if(artistTermsFrequency.contains("\""))
			{
				finalArtistTermsFreq = finalArtistTermsFreq.replaceAll("\"", "");
			}
			
			String[] artistTermsFreqArray = finalArtistTermsFreq.split(",");			
			artistName = field[11];
			int topTerm = 0;
			int lenCheck = finalArtistTermsFreq.length();
			
			if(lenCheck > 0)
			{
				for (int i = 0; i < artistTermsArray.length; i++)
				{
					if(isDouble(artistTermsFreqArray[i]) && isDouble(artistTermsFreqArray[topTerm]))
					{
						if(Double.parseDouble(artistTermsFreqArray[i]) > Double.parseDouble(artistTermsFreqArray[topTerm]))
						{
							topTerm = i;
						}
					}
				}
			}			
			
			if(!artistName.equals("artist_name"))
			{
				context.write(new Text(artistName), new Text(artistTermsArray[topTerm]));
			}
		}

		//context.write(new Text(finalArtistTerms), one);
		
		/*if(!finalString.contains("\""))
		{
			if(finalString != "")
			{
				context.write(new Text(finalString), one);
			}
		}*/
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
