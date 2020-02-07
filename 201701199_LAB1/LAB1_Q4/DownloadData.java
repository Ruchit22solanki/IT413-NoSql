package DownloadData;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class DownloadData 
{
	public static class DownloadMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key, Text text, Context context) throws IOException,InterruptedException 
		{
			int index=0,f_length=0;
			String s_length="";
			String s = text.toString();
			while(index<s.length() && s.charAt(index)!='[')
			{
				index++;
			}
			index+=4;
			String month=String.valueOf(s.charAt(index))+String.valueOf(s.charAt(index+1))+String.valueOf(s.charAt(index+2));
			index+=4;
			String year=String.valueOf(s.charAt(index))+String.valueOf(s.charAt(index+1))+String.valueOf(s.charAt(index+2))+String.valueOf(s.charAt(index+3));
			text.set(month+"-"+year);
			while(index<s.length() && s.charAt(index)!='"')
			{
				index++;
			}
			index++;
			while(index<s.length() && s.charAt(index)!='"')
			{
				index++;
			}
			index++;
			while(index<s.length() && s.charAt(index)!=' ')
			{
				index++;
			}
			index+=2;
			while(index<s.length() && s.charAt(index)!=' ')
			{
				index++;
			}
			index++;
			if(s.charAt(index)!='-')
			{
				while(index<s.length() && s.charAt(index)!=' ')
				{
					s_length+=s.charAt(index);
					index++;
				}
				f_length=Integer.parseInt(s_length);
			}
			
			context.write(text ,new IntWritable(f_length));
		}
	}
	
	public static class downloadReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		public void reduce(Text text, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException 
		{
			int final_sum=0,count=0;
			for(IntWritable value: values)
			{
				final_sum=final_sum+value.get();
				count++;
			}
			String s_count=Integer.toString(count);
			String s_text = text.toString();
			text.set(s_text+" "+s_count);
			context.write(text, new IntWritable(final_sum));
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		Configuration configuration_data= new Configuration();
		Job job = Job.getInstance(configuration_data,"dataSize");
		job.setJarByClass(DownloadData.class);
		
		job.setMapperClass(DownloadMapper.class);
		job.setReducerClass(DownloadReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path output_path = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		output_path.getFileSystem(configuration_data).delete(output_path,true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
