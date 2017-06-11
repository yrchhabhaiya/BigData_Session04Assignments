package assignment41;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TelevisionReducer extends Reducer<Text, Text, Text, Text>
{	
	public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException
	{
			context.write(key, (Text) values);
			
	}
}
