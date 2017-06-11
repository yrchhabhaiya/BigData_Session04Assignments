package assignment422;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
//import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TelevisionReducer extends Reducer<Text, Text, Text, IntWritable> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
	int count = 0;
		
		for (Text i : values){
			count++;
		}
		
		context.write(key, new IntWritable(count));
	}
}
