package assignment41;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TelevisionMapper extends Mapper<LongWritable, Text, Text, Text> {
	
		Text companyName;
		Text ProductName;
		Text details;
		Text cpName;
		Text na;
		
		public void setup(Context context){
			companyName = new Text();
			ProductName = new Text();
			details = new Text();
			cpName = new Text();
			na = new Text("NA");
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String[] lineArray = value.toString().split("\\|");
			
			companyName.set(lineArray[0]);
			ProductName.set(lineArray[1]);
			details.set(lineArray[2] + "|" + lineArray[3] + "|" + lineArray[4]  + "|" + lineArray[5]);
			//context.write(companyName, details);
			
			
			if(!companyName.equals(na) && !ProductName.equals(na)){
				cpName.set(companyName.toString() + "|" + ProductName.toString() + "|");
				context.write(cpName, details);			
			}
			
		}
}
