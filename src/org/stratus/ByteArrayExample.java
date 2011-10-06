package org.stratus;
 
import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

 
public class ByteArrayExample {
    
    // input key, value:           (don't care, hexstring)
    // intermediate key, value:    (


    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, BytesWritable, IntWritable> {

	private final static IntWritable zero = new IntWritable(0);  // anything for value
 
	public void map(LongWritable key, Text value, OutputCollector<BytesWritable, IntWritable> output, Reporter reporter) throws IOException {

	    String hexstr = value.toString();
	    int len = hexstr.length();

	    BytesWritable difficulty;
	    	    
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
		data[i / 2] = (byte) ((Character.digit(hexstr.charAt(i), 16) << 4) + Character.digit(hexstr.charAt(i+1), 16));
	    }

	    difficulty = new BytesWritable(data);
	    output.collect(difficulty, zero);           // BytesWritable, IntWritable = 0
	}
    }


 
    public static class Reduce extends MapReduceBase implements Reducer<BytesWritable, IntWritable, Text, IntWritable> {
	public void reduce(BytesWritable key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    int sum = 0;

	    while (values.hasNext()) {
		sum += values.next().get();
	    }

	    output.collect(new Text(key.toString()), new IntWritable(sum));
	}
    }
 

    public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf(ByteArrayExample.class);


	conf.setJobName("bytearray");
	conf.setBoolean("mapred.output.compress", false);
	conf.setNumMapTasks(1);
	conf.setNumReduceTasks(1);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	conf.setOutputKeyClass(BytesWritable.class);  // intermediate
	conf.setOutputValueClass(IntWritable.class);
 
	conf.setMapperClass(Map.class);
	conf.setReducerClass(Reduce.class);
 
	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 
	JobClient.runJob(conf);
    }
}
