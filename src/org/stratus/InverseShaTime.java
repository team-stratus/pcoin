package org.stratus;
 
import java.io.IOException;
import java.util.*;
import java.util.regex.*;
import java.util.Date;
// import java.security.MessageDigest; 
import java.security.*;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

// Test harness for doing scale-out timings of Inverse SHA 256 for bitcoin generations.  We hardcode the difficulty.

public class InverseShaTime {
    
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, IntWritable> {

	// take a hex string, such as a representation of a SHA 256, and return as an array of bytes

	private static byte[] hexStringToBytes(String str) {
	    int len = str.length(); 
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
	    	data[i / 2] = (byte) ((Character.digit(str.charAt(i), 16) << 4) + Character.digit(str.charAt(i+1), 16));
	    }
	    return data;
	}
		
	// take a hex string, such as a representation of a SHA 256, and return as a Hadoop BytesWritable

	private static BytesWritable hexStringToBytesWritable(String hexstr) {
	    return new BytesWritable(hexStringToBytes(hexstr));
	}

	private static MessageDigest initializeSha256(String match_data) throws IOException {
	    byte[] data = hexStringToBytes(match_data);
	    MessageDigest digest = null;
	    try {
		digest = MessageDigest.getInstance("SHA-256");
		digest.reset();
		digest.update(data);
	    } catch (NoSuchAlgorithmException e) { 
		throw new IOException("can't initialize sha 256");
	    }
	    return digest;
	}
	
	private static final int NUMBER_OF_RUNS = 1000000;


	private static final String[] hexArray = {
	    "00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f",
	    "10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f",
	    "20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f",
	    "30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f",
	    "40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f",
	    "50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f",
	    "60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f",
	    "70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f",
	    "80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f",
	    "90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f",
	    "a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af",
	    "b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf",
	    "c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf",
	    "d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df",
	    "e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef",
	    "f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"};

	private static Text bytesToHexString(byte[] data) {

	    StringBuffer hexString = new StringBuffer();
	    for (int i = 0; i < data.length; i++) {
		hexString.append(hexArray[0xFF & data[i]]);
	    }
	    return new Text(hexString.toString());
	}

	private static byte[] longToBytes(long number) {
	    int len;

	    if (number == 0) {
		len = 1;
	    } else {
		// get number of places base 16
		len = (int) Math.ceil(Math.log10(number) / Math.log10(256) + 0.000000001);
	    }

	    byte[] data = new byte[len];

	    for (int i = len - 1; i >= 0; i--) {
		data[i] = (byte) (number % 256);
		number = number/256;
	    }

	    return data;
	}

	private final static IntWritable one = new IntWritable(1);

	public static byte[] doubleSha(MessageDigest md, byte[] ba) {
    	
	    md.update(ba);
	    byte [] digest = md.digest();			
	    md.reset();				
	    md.update(digest);
	    return md.digest();								    	
	}


	public void map(LongWritable key, Text text, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException {
	    
	    String parts[] = text.toString().split(":");   // e.g.  1000000000:e8d6829a8a21adc5d3...bd1d:00000000000044b9f200...000  => nonce-start, match, target

	    long start_nonce   = Long.parseLong(parts[0], 10);
	    long nonce         = start_nonce;

	    String source_text = parts[1];

	    BytesWritable target = hexStringToBytesWritable(parts[2]);	

	    MessageDigest prototype_digest = initializeSha256(source_text);
	    byte[] data;

	    int  hits  = 0;
	    Date start = new Date();
	    
	    while (true) {

		try {
		    data = doubleSha((MessageDigest) prototype_digest.clone(), longToBytes(nonce));
		} catch (CloneNotSupportedException e) {              
		    throw new IOException("can't clone prototype digest");
		}
		
		BytesWritable candidate = new BytesWritable(data);
		
		if (candidate.compareTo(target) < 0) {
		    hits++;
		}

		if (nonce > start_nonce + NUMBER_OF_RUNS) {
		    break;
		} else {
		    nonce++;
		}
	    }

	    long elapsed_milliseconds = (new Date()).getTime() - start.getTime();

	    output.collect(new LongWritable(nonce), new IntWritable((int) elapsed_milliseconds));
	    // output.collect(new LongWritable(nonce), new IntWritable(hits));
	}
    }
 
    // Identity - if we have multiple values, it's an error.  Every map task gets its own nonce.

    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
	public void reduce(LongWritable key, Iterator<IntWritable> values, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException {
	    output.collect(key, new IntWritable(values.next().get()));
	}
    }
 
    public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf(InverseShaTime.class);

	conf.setJobName("inverse sha256");
	conf.setBoolean("mapred.output.compress", false);

	conf.setNumMapTasks(5);
	conf.setNumReduceTasks(1);
 
	conf.setOutputKeyClass(LongWritable.class);
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
