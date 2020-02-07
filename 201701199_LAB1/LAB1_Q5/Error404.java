package Error404;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class Error404 
{
	public static class ErrMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key, Text text, Context context) throws IOException,InterruptedException 
		{
			int index=0;
			string t_stamp="";
			String s = text.toString();
			while(index<s.length() && s.charAt(index)!='[')
			{
				index++;
			}
			index++;
			while(index<s.length() && s.charAt(index)!=':')
			{
				t_stamp+=s.charAt(index);
				index++;
			}
			index++;
			t_stamp+=" ";
			while(index<s.length() && s.charAt(index)!=']')
			{
				t_stamp+=s.charAt(index);
				index++;
			}
			while(index<s.length() && s.charAt(index)!='/')
			{
				index++;
			}
			t_stamp+=" ";
			while(index<s.length() && s.charAt(index)!=' ')
			{
				t_stamp+=s.charAt(index);
				index++;
			}
			while(index<s.length() && s.charAt(index)!='"')
			{
				index++;
			}
			index+=2;
			if(s.charAt(index+1)=='0' && s.charAt(index+2)=='4' && s.charAt(index)=='4')
			{
				text.set(t_stamp);
				context.write(text, new IntWritable(1));
			}
		}
	}
	
	public static class ErrReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		public void reduce(Text text, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException 
		{
			
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		Configuration configuration_err= new Configuration();
		Job job = Job.getInstance(configuration_err,"error404");
		job.setJarByClass(Error404.class);
		job.setNumReduceTasks(0);
		
		job.setMapperClass(ErrMapper.class);
		job.setReducerClass(ErrReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path output_path = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		output_path.getFileSystem(configuration_err).delete(output_path,true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
