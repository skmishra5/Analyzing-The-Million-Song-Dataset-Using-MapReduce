package MillionSongAnalysis;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class EightthReducer extends Reducer<Text, Text, Text, Text>{
	
	private TreeMap<Integer, String> tempoList = new TreeMap<Integer, String>();

	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;	
	
		for (Text val: values)
		{
			count++;
		}
		
		tempoList.put(count, key.toString());
		
		if(tempoList.size()>10){		 
			tempoList.remove(tempoList.firstKey());
	    }
		//context.write(new Text(String.valueOf(count)), key);
	}
	
	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
		Set set = tempoList.entrySet();
	    Iterator i = set.iterator();
	    
	    while(i.hasNext()) {
	    	Map.Entry me = (Map.Entry)i.next();
	    	context.write(new Text(me.getValue().toString()), new Text(me.getKey().toString()));
	    }
		
  	}

}
