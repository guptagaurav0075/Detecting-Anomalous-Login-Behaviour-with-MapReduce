package MapReduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MRmapper2  extends Mapper <LongWritable,Text,Text,Text> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
	// TODO: write (key, value) pair to context (hint: need to be clever here)
		String[] Vals = value.toString().split("\\s+");
		
	// TODO: write (acctname, 1) to context
		
	   if(Vals.length==2){
			String textKey = Vals[0];
			String intValue = Vals[1];
			String combinedValue = textKey+"@_@"+intValue;

		// TODO: write (acctname, 1) to context
			context.write(new Text("summary"), new Text(combinedValue));
		}
			
	}
}
