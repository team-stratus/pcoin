package org.stratus;
 
import java.io.IOException;
import java.util.*;
import java.util.regex.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 
public class ByteArrayExample {
    
    //                 KEY              VALUE
    //
    // INPUT           D/C              String (SHA-256 hex)
    // INTERMEDIATE    BytesWritable    IntWritable
    // OUTPUT          Text             IntWritable


    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, BytesWritable, IntWritable> {

	private final static IntWritable one = new IntWritable(1);

	// Take a string representing a SHA-256, e..g "2c4a2c56ba6e3fd88ee27d0348f3fc5e6fab02e68d76f526c7c2d7055d7f7ea7",
	// and return it as a BytesWritable (32 bytes, 256 bits)

	private static BytesWritable textToBytesWritable(Text hextext) {
	    String hexstr = hextext.toString();
	    int len = hexstr.length();
	    	    
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
		data[i / 2] = (byte) ((Character.digit(hexstr.charAt(i), 16) << 4) + Character.digit(hexstr.charAt(i+1), 16));
	    }
	    return new BytesWritable(data);
	}

 
	public void map(LongWritable key, Text value, OutputCollector<BytesWritable, IntWritable> output, Reporter reporter) throws IOException {
	    output.collect(textToBytesWritable(value), one);           // BytesWritable, IntWritable
	}
    }


    public static class Reduce extends MapReduceBase implements Reducer<BytesWritable, IntWritable, Text, IntWritable> {

	private static Pattern pattern = Pattern.compile(" "); 
	private static Matcher matcher = pattern.matcher("");

	public void reduce(BytesWritable key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    int sum = 0;
	    
	    while (values.hasNext()) {
		sum += values.next().get();
	    }

	    // We want to remove the spaces that key.toString() inserts in SHA-256 representation, e.g.
	    // "2c 4a 2c 56 ba 6e 3f d8 8e e2 7d 03 48 f3 fc 5e 6f ab 02 e6 8d 76 f5 26 c7 c2 d7 05 5d 7f 7e a7"
	    // now becomes the unreadble "2c4a2c56ba6e3fd88ee27d0348f3fc5e6fab02e68d76f526c7c2d7055d7f7ea7".

	    matcher.reset(key.toString());
	    output.collect(new Text(matcher.replaceAll("")), new IntWritable(sum));
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

	conf.setOutputKeyClass(BytesWritable.class);  // Intermediate
	conf.setOutputValueClass(IntWritable.class);
 
	conf.setMapperClass(Map.class);
	conf.setReducerClass(Reduce.class);
 
	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 
	JobClient.runJob(conf);
    }
}
