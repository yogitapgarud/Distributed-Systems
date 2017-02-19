import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class Mp2 {
	static int unique=0;
	static int total=0;

	public static void main(String [] args) throws Exception
	{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Path output_new= new Path(files[2]);
		Job j = Job.getInstance(c,"wordcount");
		j.setJarByClass(Mp2.class);
		j.setMapperClass(MapForMp2.class);
		j.setReducerClass(ReduceForMp2.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		j.waitForCompletion(true);

		Configuration c1=new Configuration();
		Job j2=new Job(c1,"wordcount1");
		j2.setJarByClass(Mp2.class);
		j2.setMapperClass(Map2ForMp2.class);
		j2.setReducerClass(Reduce2ForMp2.class);
		j2.setOutputKeyClass(Text.class);
		j2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j2, output);
		FileOutputFormat.setOutputPath(j2,output_new );
		j2.waitForCompletion(true);
		//System.out.println("unique words: "+unique);
		//System.out.println("total words: "+total);
		System.exit(0);
	}


	public static class MapForMp2 extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] words=line.split(",");
			int len = words.length;
			for(int i = 0; i < len; i++)
			{
				for(int j = i+1; j < len; j++)
				{
					String str1 = words[i].trim();
					String str2 = words[j].trim();
					Text outputKey = new Text();

					if (str1.compareTo(str2) < 0)
						outputKey.set("(" + str1 + ", " + str2 + ")");
					else
						outputKey.set("(" + str2 + ", " + str1 + ")");
					IntWritable outputValue = new IntWritable(1);
					con.write(outputKey, outputValue);
				}
			}
		}
	}


	public static class Map2ForMp2 extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] words=line.split("\\)");
			String new_word=words[1].replaceAll("\\s+|\\s+$","");

			int total=Integer.parseInt(new_word);
			String word=words[0];
			Text outputKey = new Text(word);
			IntWritable outputValue = new IntWritable(Integer.parseInt(new_word));
			con.write(outputKey,outputValue);
			String unique="Unique pairs :";
			outputKey.set(unique);
			outputValue.set(1);
			con.write(outputKey,outputValue);
			String t="Total pairs :";
			outputKey.set(t);
			outputValue.set(total);
			con.write(outputKey,outputValue);
		}
	}
	public static class ReduceForMp2 extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			int sum = 0;
			//int k=0;
			for(IntWritable value : values)
			{
				sum += value.get();
				//if(k==0) {
				//	unique+=1;
				//}
				//k=1;
				total+=1;
			}
			con.write(word, new IntWritable(sum));
		}
	}

	public static class Reduce2ForMp2 extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			int sum = 0;
			//int k=0;
			String line = word.toString();
			String new_string=null;
			if(line.equals("Unique pairs :") || line.equals("Total pairs :")) {
				new_string=line;
			} else {
				new_string=line+")";
			}
			Text text=new Text();
			text.set(new_string);
			for(IntWritable value : values)
			{
				sum += value.get();
				//if(k==0) {
				//  unique+=1;

				//}
				//total+=1;
				//k=1;
			}
			con.write(text, new IntWritable(sum));
		}

	}

}
