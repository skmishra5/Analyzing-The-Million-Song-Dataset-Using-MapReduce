package MillionSongAnalysis;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// Top ten songs as per their hotness in each genre along with the artist name and title of the song.

public class FifthMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] field = value.toString().split("\t");
		
		String hottness = field[42];
		
		if(isDouble(hottness))
		{
			if((Double.parseDouble(hottness) > 0) && ((Double.parseDouble(hottness) < 1.0)))
			{
				String title = field[50];
				String artistName = field[11];
				
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
						if(!artistTermsArray[topTerm].equals(""))
						{
							context.write(new Text(artistTermsArray[topTerm]), new Text(hottness + "----" + artistName + "----" + title));
						}
					}
				}
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
