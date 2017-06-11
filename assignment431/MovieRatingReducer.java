package assignment431;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable>{

	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
	
		int count = 0;
		for(Text value : values){
			count++;
		}
		
		context.write(key, new IntWritable(count));
	}
	
}
