package MillionSongAnalysis;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FifthReducer extends Reducer<Text, Text, Text, Text>{
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String valFinal = "";
		TreeMap<Double, String> mainList = new TreeMap<Double, String>();
		//TreeMap<Double, String> secondList = new TreeMap<Double, String>();
		
		for (Text val: values)
		{
			//valFinal += val.toString();
			String[] token = val.toString().split("----");
			//if(isDouble(token[0]))
			//{
					mainList.put(Double.parseDouble(token[0]), val.toString());
				
				//secondList.put(Double.parseDouble(token[0]), token[2]);
			
				if(mainList.size()>10){		 
					mainList.remove(mainList.firstKey());
					//secondList.remove(secondList.firstKey());
				}
			//}
			//context.write(new Text(token[0]), val);
		}
		
		Set set = mainList.entrySet();
	    Iterator i = set.iterator();
	    
	    //Set set1 = secondList.entrySet();
	    //Iterator i1 = set1.iterator();
	    
	    while(i.hasNext()) {
	    	Map.Entry me = (Map.Entry)i.next();
	    	//Map.Entry me1 = (Map.Entry)i1.next();
	    	
	    	context.write(new Text(key), new Text(me.getValue().toString()));
	    }
		
	    mainList.clear();
	    //secondList.clear();
		
	    
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
