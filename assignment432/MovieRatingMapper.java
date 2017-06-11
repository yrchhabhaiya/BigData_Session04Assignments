package assignment432;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieRatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	IntWritable movieId;
	Text details;
	
	public void setup(Context context){
		movieId = new IntWritable();
		details = new Text();
	}
	public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		
		String[] lineArray = values.toString().split("\t");
		movieId = new IntWritable(Integer.parseInt(lineArray[1]));
		details.set("\t" + lineArray[0] + "\t" + lineArray[2] + "\t" + lineArray[3]);
		context.write(movieId, details);
	}
	
}
