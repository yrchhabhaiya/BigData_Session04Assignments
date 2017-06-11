package assignment422;

import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TelevisionMapper extends Mapper<LongWritable, Text, Text, Text> {

	Text companyName;
	Text details;
	Text productName;
	//Text cpName;
	public static final Text NA = new Text("NA");
	
	public void setup(Context context){
		companyName = new Text();
		details = new Text();
		productName = new Text();
		//cpName = new Text();
	}
	

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] lineArray = value.toString().split("\\|");
		
		companyName.set(lineArray[0]);
		productName.set(lineArray[1]);
		details.set( lineArray[1] + "|" + lineArray[2] + "|" +lineArray[3] + "|" +lineArray[4] + "|" +lineArray[5]);
		
		//if(!companyName.equals(NA) && !productName.equals(NA)){
		//	details.set("|" + productName.toString() + details.toString());
			context.write(companyName, details);
	//	}
	
	}
}
