import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;


enum Temperature {
	DISCARDED
}

public class MaxTemperatureMapper extends
		Mapper<LongWritable, Text, Text, FloatWritable> {
	
	
	private TemperatureParser parser = new TemperatureParser();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		
		parser.parse(value);
		if ("0000".equals(parser.getHour())) {
		context.write(new Text(parser.getMonth()), 
					new FloatWritable(parser.getMaxTemperature()));
		}
		else
			context.getCounter(Temperature.DISCARDED).increment(1);
		
		
	}

}
