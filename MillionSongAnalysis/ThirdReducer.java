package MillionSongAnalysis;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ThirdReducer extends Reducer<Text, Text, Text, Text>{
	
	private TreeMap<Double, String> tempoList = new TreeMap<Double, String>();
	private ArrayList<Double> medianList = new ArrayList<Double>();
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double sum = 0.0d;
		int count = 0;
		String[] daceabilityArray = null;
		String temp = "";
		
		for (Text val: values)
		{
			tempoList.put(Double.parseDouble(val.toString()), "one");
			
			//temp += val.toString();
			//sum += Double.parseDouble(val.toString());
			//count++;
			//context.write(key, new Text(val));
			
//			if(val.toString().contains("[")){
//				daceabilityArray = val.toString().replaceAll("\\[", "").replaceAll("\\]", "").split(",");
//				for (int i = 0; i < daceabilityArray.length; i++)
//				{
//					sum += Double.parseDouble(daceabilityArray[i]); 
//					count++;
//				}
//				double averageDaceability = sum/count;
//				context.write(key, new Text(String.valueOf(averageDaceability)));
//			}
//			else
//			{
				
//			}
		}
		//context.write(key, new Text(temp));
		//double averageDaceability = sum/count;
		//if(averageDaceability > 0)
		//{
			//context.write(key, new Text(String.valueOf(averageDaceability)));
		//}
	}
	
	public int calculateMedian(int value)
	{
		if(value % 2 == 0)
		{
			return ((value/2) + 1);
		}
		else
		{
			return ((value + 1) / 2);
		}
	}

	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
		Set set = tempoList.entrySet();
	    Iterator i = set.iterator();
	    
	    while(i.hasNext()) {
	    	Map.Entry me = (Map.Entry)i.next();
	    	medianList.add(Double.parseDouble(me.getKey().toString()));
	    }
	    
	    int index = calculateMedian(medianList.size());
	    
	    context.write(new Text("Median Danceability: "), new Text(medianList.get(index).toString()));
  	}

		
	//}

}
