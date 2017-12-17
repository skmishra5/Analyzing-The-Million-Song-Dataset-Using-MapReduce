package MillionSongAnalysis;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FourthReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{
	
	private TreeMap<Double, String> tempoList = new TreeMap<Double, String>();

	protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for (Text val: values)
		{
			tempoList.put(Double.parseDouble(key.toString()), val.toString());
			if(tempoList.size()>10){		 
				tempoList.remove(tempoList.firstKey());
		    }
			//context.write(val, key);
		}

	}
	
	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
		Set set = tempoList.entrySet();
	    Iterator i = set.iterator();
	    
	    while(i.hasNext()) {
	    	Map.Entry me = (Map.Entry)i.next();
	    	context.write(new Text(me.getValue().toString()), new DoubleWritable(Double.parseDouble(me.getKey().toString())));
	    }
		
  	}
	
}
