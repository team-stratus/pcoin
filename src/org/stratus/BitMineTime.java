package org.stratus;
 
import java.io.IOException;
import java.security.*; 
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


// Hadoop Map-Reduce program for timing Bitcoin generation.  We do
// NUMBER_OF_RUNS attempts to solve the inverse hash problem, and
// return the number of milliseconds elapsed.  90% of the run time is
// taken up by the SHA-256 computations; The overhead of manipulating
// the data takes the rest. (These last include byte swapping,
// coversion of byte arrays to a comparable object, etc.)
//
// See the accompanying BitMine.Solutions for example test data, from 
// previously successful bitcoin generating.
//
// Team Stratus.

public class BitMineTime {

    private static final int NUMBER_OF_RUNS = 1000000 * 5;

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

	/**
	 * @param args
	 * @throws IOException
	 */

	// Example input text value:
	//
	// "2504433986:0100000081cd02ab7e569e8bcd9317e2fe99f2de44d49ab2b8851ba4a308000000000000e320b6c2fffc8d750423db8b1eb942ae710e951ed797f7affc8892b0f1fc122bc7f5d74df2b9441a:00000000000044b9f20000000000000000000000000000000000000000000000";		
	// We split on ":" to three pieces of data: initial-nonce as decimal string, block-header and target-difficulty as hexadecimal strings.

	public void map(LongWritable key, Text text, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {

	    String parts[]       = text.toString().split(":");

	    long start_nonce     = Long.parseLong(parts[0], 10);        // typically the start_nonce we're given for testing will be within NUMBER_OF_RUNS of a succesful nonce.
	    MessageDigest digest = initializeSha256(parts[1]);          // pre-load a digest object with data; we'll clone this for nonce computations
	    BytesWritable target = hexStringToBytesWritable(parts[2]);	// our target: we want to generate a hash less than this value

	    int  hits   = 0;
	    Date start  = new Date();
	    long nonce  = start_nonce;

	    do {
		byte[] data;

		try 
		    { data = doubleSha((MessageDigest) digest.clone(), longToBytes(nonce)); } 

	    	catch (CloneNotSupportedException e) 
 		    { throw new IOException("can't clone prototype digest"); }

	    	BytesWritable candidate = new BytesWritable(data);

	    	if (candidate.compareTo(target) < 0) hits++;

	    	nonce++;
	    	
	    } while (nonce < start_nonce + NUMBER_OF_RUNS);

	    Date now = new Date();
	    
	    // return epoch time in mlliseconds, delta in milliseconds

	    output.collect(new LongWritable(nonce), new Text(String.format("%d:%d", now.getTime(), now.getTime() - start.getTime())));
	}
    }

    // Essentially the identity reduction - if we have multiple values, it's an error.  Every map task should be getting its own nonce.
    // Produces the <final-nonce,elapsed-time> pairs for checking performance.
	
    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
	public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
	    output.collect(key, values.next());
	}
    }
 
    public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf(BitMineTime.class);

	conf.setJobName("bit miner proof of concept");
	conf.setBoolean("mapred.output.compress", false);

	conf.setNumMapTasks(100);
	conf.setNumReduceTasks(1);
 
	conf.setOutputKeyClass(LongWritable.class);
	conf.setOutputValueClass(Text.class);
 
	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);
 
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
 
	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 
	JobClient.runJob(conf);
    }

	
    // Take a hex string, such as a representation of a SHA 256, and return as an array of bytes

    private static byte[] hexStringToByteArray(String str) {
	int len = str.length(); 
	byte[] data = new byte[len / 2];
	for (int i = 0; i < len; i += 2) {
	    data[i / 2] = (byte) ((Character.digit(str.charAt(i), 16) << 4) + Character.digit(str.charAt(i+1), 16));
	}
	return data;
    }
		
    // Take a hex string, such as a representation of a SHA 256, and return as a Hadoop BytesWritable

    private static BytesWritable hexStringToBytesWritable(String hexstr) {
	return new BytesWritable(hexStringToByteArray(hexstr));
    }
	
    // Setup a SHA-256 object with initial data (block-header data).  We'll clone this for nonce computations.

    private static MessageDigest initializeSha256(String match_data) throws IOException {
	byte[] data = hexStringToByteArray(match_data);
	MessageDigest digest = null;
	try {
	    digest = MessageDigest.getInstance("SHA-256");
	    digest.reset();
	    digest.update(data);
	} catch (NoSuchAlgorithmException e) { 
	    throw new IOException("can't initialize SHA-256");
	}
	return digest;
    }
	

    // Convert a long to an array of bytes.  TODO: this seems expensive, is there a faster way?

    private static byte[] longToBytes(long number) {
	int len;

	if (number == 0) {
	    len = 1;
	} else {
	    len = (int) Math.ceil(Math.log10(number) / Math.log10(256) + 0.000000001);    // get number of places base 16
	}

	byte[] data = new byte[len];

	for (int i = 0; i < len; i++) {
	    data[i] = (byte) (number % 256);
	    number = number / 256;
	}
	return data;
    }

    // Add final data to the SHA-256 object, then perform a second SHA-256 and
    // return the swapped bytes from the completed digest.
	
    private static byte[] doubleSha(MessageDigest md, byte[] ba) {
    	
	md.update(ba);
	byte [] digest = md.digest();			
	md.reset();				
	md.update(digest);
	return swapBytes(md.digest());
    }
	
    // Dump array of bytes, for debugging
	        	
    private static String dumpByteArray(byte[] ba) {

	StringBuffer sb = new StringBuffer();
	for (int i = 0; i < ba.length; i++) {
	    sb.append(0xff & ba[i]);
	    if (i < (ba.length - 1)) sb.append(':');
	}
	return sb.toString();
    }

    // Swap bytes end for end in place; returns the original, modified byte array.

    private static byte[] swapBytes(byte[] data) {
	int offset = data.length - 1;
	byte tmp;
    	
	for (int i = 0; i < offset/2; i++) {
	    tmp = data[i];
	    data[i] = data[offset - i];
	    data[offset - i] = tmp;
	}
	return data;
    }
	
}

