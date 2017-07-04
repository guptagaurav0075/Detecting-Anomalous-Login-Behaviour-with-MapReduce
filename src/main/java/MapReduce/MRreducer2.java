package MapReduce;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.lang.Math;

public class MRreducer2  extends Reducer <Text,Text,Text,DoubleWritable> {
   public void reduce(Text key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
	
	// TODO: parse out (key, values) (based on hint of cleverness mapper)
	   HashMap vals = new HashMap();
	   int totalAttempts=0, numberOfUsers=0;
	   while(values.iterator().hasNext()){
		   String[] value = values.iterator().next().toString().split("@_@");
		   totalAttempts+=Integer.parseInt(value[1]);
		   if(value.length==2){
			   vals.put(value[0], value[1]);
			   ++numberOfUsers;
		   }
	   }
	   double mean = totalAttempts/numberOfUsers;
	   double sigma = 0;
	   Set setOfValues = vals.entrySet();
	   Iterator iterate = setOfValues.iterator();
	   while(iterate.hasNext()){
		   Map.Entry me = (Map.Entry)iterate.next();
		   double temp = mean - Double.parseDouble((String) me.getValue());
		   temp = temp*temp;
		   sigma+=temp;
	   }
	   sigma = sigma/numberOfUsers;
	   sigma=Math.sqrt(sigma);
	   // TODO: calculate num_sigmas_for:<user> and write to context
	   context.write(new Text("mean_failed_login_attempts"), new DoubleWritable(mean));
	   context.write(new Text("sigma_failed_login_attempts"), new DoubleWritable(sigma));
	   iterate = setOfValues.iterator();
	   
	   while(iterate.hasNext()){
		   Map.Entry me = (Map.Entry)iterate.next();
		   double temp = Double.parseDouble((String) me.getValue())-mean;
		   temp = temp/sigma;
		   context.write(new Text("num_sigmas_for:"+me.getKey()), new DoubleWritable(temp));
	   }
   }
}
