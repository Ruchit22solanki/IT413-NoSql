package MaxTemp;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MaxTemperature
{
	public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		private static final int INF_Temp = 9999;
		public void map(LongWritable key, Text text, Context context) throws IOException,InterruptedException
		{
			int Air_Temperature;
			String Line = text.toString();
			String Year = Line.substring(15, 19);
			if (Line.charAt(87) == '+') 
			{ 
				Air_Temperature = Integer.parseInt(Line.substring(88, 92));
			} 
			else 
			{
				Air_Temperature = Integer.parseInt(Line.substring(87, 92));
			}
			String Air_Quality = Line.substring(92, 93);
			if (Air_Temperature != INF_Temp && Air_Quality.matches("[01459]")) 
			{
				context.write(new Text(Year), new IntWritable(Air_Temperature));
			}
		}
	}
	
	public static class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
	  
	  public void reduce(Text text, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	    {
	    
            int MaximumValue = Integer.MIN_VALUE;
            for (IntWritable value : values) 
            {
                MaximumValue = Math.max(MaximumValue, value.get());
            }
            context.write(text, new IntWritable(MaximumValue));
	    }
	}

	public static void main(String[] args) throws Exception
	{
	    if (args.length != 2) 
	    {
	      System.exit(-1);
	    }
	    
	    Configuration configuration_temp= new Configuration();
	    Job job = Job.getInstance(configuration_temp,"maxTemp");
	    job.setJarByClass(MaxTemperature.class);
	    
	    job.setMapperClass(MaxTemperatureMapper.class);
	    job.setReducerClass(MaxTemperatureReducer.class);
	
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path output_path = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
        output_path.getFileSystem(configuration_temp).delete(output_path,true);
		
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  
	}

}