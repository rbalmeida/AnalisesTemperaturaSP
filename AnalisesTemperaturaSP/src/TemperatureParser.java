import org.apache.hadoop.io.Text;




public class TemperatureParser {

	
	private String month;
	private String hour;
	private float minTemperature;
	private float maxTemperature;
	

	
	public void parse(String record){
		
		String[] values = record.split(";");		
		month = values[1].substring(3, 5);
		hour = values[2];
		
		if("0000".equals(hour))		
			maxTemperature = Float.parseFloat(values[4]);
		else				
			minTemperature = Float.parseFloat(values[values.length-1]);
		
	}
	
	public void parse(Text record){
		parse(record.toString());
	}
	

	public String getMonth(){
		return month;
	}
	
	public float getMinTemperature() {
		return minTemperature;
	}

	public float getMaxTemperature() {
		return maxTemperature;
	}

	public String getHour(){
		return hour;
	}
	
	
}
