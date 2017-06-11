package assignment431;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieRatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	IntWritable userId;
	Text details;
	
	public void setup(Context context){
		userId = new IntWritable();
		details = new Text();
	}
	
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		
		String[] lineArray = values.toString().split("\t");
		userId.set(Integer.parseInt(lineArray[0]));
		details.set("\t" + lineArray[1] + "\t" + lineArray[2] + "\t" + lineArray[3]);
		context.write(userId, details);
	}
	
}
