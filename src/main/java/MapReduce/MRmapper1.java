package MapReduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MRmapper1  extends Mapper <LongWritable,Text,Text,IntWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	// TODO: filter failed USER_LOGIN records, discard the rest
		String Line = value.toString();
		String initialPattern = "type=USER_LOGIN";
		Pattern pattern = Pattern.compile(initialPattern);
		Matcher matcher = pattern.matcher(Line);
		if(matcher.find()){//checks type=USER_LOGIN
			String initialPattern1 = "(res=failed)";
			Pattern pattern1 = Pattern.compile(initialPattern);
			Matcher matcher1 = pattern1.matcher(Line);
			if(matcher1.find()){ //checks if the login was failed
	// TODO: discard records with acct name that do NOT have ""
				String intialPattern2 = "acct=\"[(]?\\w+[)]?\"";
				Pattern pattern2 = Pattern.compile(intialPattern2);
				Matcher matcher2 = pattern2.matcher(Line);
				if(matcher2.find()){//checks if 
	// TODO: write (acctname, 1) to context
					context.write(new Text(matcher2.group(0).replaceFirst("acct=", "")), new IntWritable(1));
				}
			}
		}
	
	}
}
