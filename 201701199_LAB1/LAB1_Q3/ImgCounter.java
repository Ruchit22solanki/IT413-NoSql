package ImgCounter;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class ImgCounter 
{
	public static class ImgMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key, Text text, Context context) throws IOException,InterruptedException 
		{
			String Line = text.toString();
			if(Line.contains(".png"))
			{
				text.set(".png");
				context.write(text, new IntWritable(1));
			}
			else if(Line.contains(".gif"))
			{
				text.set(".gif");
				context.write(text, new IntWritable(1));
			}
			else
			{
				text.set("other");
				context.write(text, new IntWritable(1));
			}
		}
	}
	
	public static class ImgReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		public void reduce(Text text, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException 
		{
			int final_sum=0;
			
			for(IntWritable value: values)
			{
				final_sum=final_sum+value.get();
			}
			context.write(text, new IntWritable(final_sum));
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		Configuration configuration_img= new Configuration();
		Job job = Job.getInstance(configuration_img,"wordCount");
		job.setJarByClass(ImgCounter.class);
		
		job.setMapperClass(ImgMapper.class);
		job.setReducerClass(ImgReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path output_path = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		output_path.getFileSystem(configuration_img).delete(output_path,true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}