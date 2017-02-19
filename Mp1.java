import java.io.*;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.*;
import java.net.URI;
public class Mp1 {
	//static int unique=0;
	//static int total=0;
	static int index= 1;
	public static void main(String [] args) throws Exception
	{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Path output_new=new Path(files[2]);	
		
		Job j=new Job(c,"wordcount");
		j.setJarByClass(Mp1.class);
		j.setMapperClass(MapForMp1.class);
		j.setReducerClass(ReduceForMp1.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		j.waitForCompletion(true);
		
		Configuration c1=new Configuration();
		Job j2=new Job(c1,"wordcount1");
		j2.setJarByClass(Mp1.class);
		j2.setMapperClass(Map2ForMp1.class);
		j2.setReducerClass(Reduce2ForMp1.class);
		j2.setOutputKeyClass(Text.class);
		j2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j2, output);
		FileOutputFormat.setOutputPath(j2,output_new );
		j2.waitForCompletion(true);
		//System.out.println("unique words: "+unique);
		//System.out.println("total words: "+total);

	}

	public static class MapForMp1 extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] words=line.split(" ");
			
			for(String word: words )
			{
				String result = word.replaceAll("[-+.^:,]","");
				Text outputKey = new Text(result.toLowerCase().trim());
				IntWritable outputValue = new IntWritable(1);
				con.write(outputKey, outputValue);
			}
		}
	}

	public static class Map2ForMp1 extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] words=line.split("\\s+");
				
			int total=Integer.parseInt(words[1]);
			String word=words[0];
			Text outputKey = new Text(word);
			IntWritable outputValue = new IntWritable(Integer.parseInt(words[1]));
			con.write(outputKey,outputValue);
			String unique="~unique words :";
			outputKey.set(unique);
			outputValue.set(1);
			con.write(outputKey,outputValue);
			String t="~total words :";
			outputKey.set(t);
			outputValue.set(total);
			con.write(outputKey,outputValue);
		}
	}

	public static class ReduceForMp1 extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			int sum = 0;
			//int k=0;
			//String s=""+index+") "+word.toString();
			//Text new_word=new Text();
			//ew_word.set(s);
			for(IntWritable value : values)
			{
				sum += value.get();
				//if(k==0) {
				//	unique+=1;

				//}
				//total+=1;
				//k=1;
			}
			con.write(word, new IntWritable(sum));
			//index++;
		}
		

	}

	public static class Reduce2ForMp1 extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			int sum = 0;
			//int k=0;
			String s=word.toString();
			String s2=null;
			if(s.equals("~unique words :") || s.equals("~total words :")) {
				s2 = s.replaceAll("[-+.^:,~]","");
				//s2=word.toString();
			} else {
				s2=""+index+" "+word.toString();	
				index++;
			}
			Text new_word=new Text();
			new_word.set(s2);
			for(IntWritable value : values)
			{
				sum += value.get();
				//if(k==0) {
				//	unique+=1;

				//}
				//total+=1;
				//k=1;
			}
			con.write(new_word, new IntWritable(sum));
		}
		
	}

}
