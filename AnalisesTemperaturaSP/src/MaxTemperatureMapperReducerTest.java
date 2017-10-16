

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

public class MaxTemperatureMapperReducerTest {

	@Test
	public void processesValidMaxTempRecord() throws IOException, InterruptedException {
		Text value = new Text("83781;01/01/2000;0000;;24.7;;");
		
		new MapDriver<LongWritable, Text, Text, FloatWritable>()
		.withMapper(new MaxTemperatureMapper())
		.withInput(new LongWritable(0), value)
		.withOutput(new Text("01"), new FloatWritable(Float.parseFloat("24.7")))
		.runTest();
				
	
	}
	
	@Test
	public void discardMinTempRecord()throws IOException, InterruptedException {
		Text value = new Text("83781;01/01/2000;1200;16.8;;19.7;");

		new MapDriver<LongWritable, Text, Text, FloatWritable>()
		.withMapper(new MaxTemperatureMapper())
		.withInput(new LongWritable(0), value)
		.runTest();
				
	
	}
	
	@Test
	public void returnsMaximumFloatrInValues() throws IOException, InterruptedException{
	
		new ReduceDriver<Text, FloatWritable, Text, FloatWritable>()
		.withReducer((new MaxTemperatureReducer()))
		.withInput(new Text("01"),
		 Arrays.asList(new FloatWritable(Float.parseFloat("24.7")),  new FloatWritable(Float.parseFloat("30.7")),  new FloatWritable(Float.parseFloat("10.7"))))
		 .withOutput(new Text("01"),  new FloatWritable(Float.parseFloat("30.7")))
		 .runTest();
		
	}
	
	
	
}
