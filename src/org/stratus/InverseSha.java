package org.myorg;
 
import java.io.IOException;
import java.util.*;
import java.util.regex.*;
import java.security.MessageDigest; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 



public class InverseSha {
 
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	//                            key
	public void map(BytesWritable hashBlock, BytesWriteable difficulty, OutputCollector<BytesWritable, BytesWritable> output, Reporter reporter) throws IOException {

	    MessageDigest md = MessageDigest.getInstance("SHA-256");

	    md.update(headerBytes, 0, 80);

	    byte[] nonce = { 0, 0, 0, 0 };

	    do {
		MessageDigest trial = md.clone();
		nonce += 1;
		trial.update(nonce, 0, 4);

	    } while (md.digest >= difficulty);

	    output.collect(difficulty, nonce);
	}
    }
 
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    int sum = 0;
	    while (values.hasNext()) {
		sum += values.next().get();
	    }
	    output.collect(key, new IntWritable(sum));
	}
    }
 
    public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf(WordCount.class);

	conf.setJobName("inverse sha256");
	conf.setBoolean("mapred.output.compress", false);

	conf.setNumMapTasks(1);
	conf.setNumReduceTasks(1);
 
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);
 
	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);
 
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
 
	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 
	JobClient.runJob(conf);
    }
}
