package org.stratus;

import java.io.IOException;
import java.security.*; 
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.stratus.zk.*;


// Hadoop Map-Reduce program for Bitcoin generation.  We do
// ....
// ....
// ....

// See the accompanying BitMine.Solutions for example test data, from 
// previously successful bitcoin generating.
//
// Team Stratus.

public class BitMine {

    private static final int INTERVAL_ITERATIONS = 5000000;  // tune this to perform one inner loop below

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, IntWritable> {

	// Example input text value:  "2504433986/192.168.2.1:2181"
	// we split on "/" to get our initial nonce, and a zookeeper host address to contact for updates.
	
	public void map(LongWritable key, Text text, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException {

	    String parts[]       = text.toString().split("/");
	    long   start_nonce   = Long.parseLong(parts[0], 10);        // each map task gets its own share of the search space (which is very large)
	    String host_address  = parts[1];                            
	    ZooKeeperCommunicator zooKeeperCommunicator = null;

	    zooKeeperCommunicator = zooKeeperCommunicator.setUp(host_address, start_nonce);
	    String config = zooKeeperCommunicator.getConfig();

	    if (config == null) {   // we're shutting down in this cae
		return;
	    }
	    
	    // We get two parts from the configuration: a block header, and a target difficulty
	    //
	    // "0100000081cd02ab7e569e8bcd9317e2fe99f2de44d49ab2b8851ba4a308000000000000e320b6c2fffc8d750423db8b1eb942ae710e951ed797f7affc8892b0f1fc122bc7f5d74df2b9441a"
	    // "00000000000044b9f20000000000000000000000000000000000000000000000"

	    String block_header  =  config.split("/")[0];
	    String target_header =  config.split("/")[1];
	    
	    MessageDigest digest = initializeSha256(block_header);            // pre-load a digest object with data; we'll clone this for nonce computations
	    BytesWritable target = hexStringToBytesWritable(target_header);   // our target: we want to generate a hash less than this value

	    Date start  = new Date();
	    long nonce  = start_nonce;
	    byte[] data;
	    int  interval_iterations;
	    boolean solution_found = false;

	    while (true) {

		// we do the inner loop for several seconds, then check for changed configurations

		interval_iterations = 0;
		do {

		    // if we've already found a solution, and the problem hasn't changed, then sleep a second and
		    // skip the computation (but check for a new configuration later, and reset if we got one).

		    if (solution_found) {
			try { 
			    Thread.sleep(1000);
			    break;
			} catch (InterruptedException e) {
			    throw new IOException("Unexpected interrupt in sleep: " + e.getMessage());  // "Can't happen"
			}
		    }

		    nonce++;
		    interval_iterations++;

		    try 
			{ data = doubleSha((MessageDigest) digest.clone(), longToBytes(nonce)); } 

		    catch (CloneNotSupportedException e) 
			{ throw new IOException("can't clone prototype digest"); }

		    BytesWritable candidate = new BytesWritable(data);

		    if (candidate.compareTo(target) < 0) {
			zooKeeperCommunicator.putSolution(candidate.toString().replaceAll(" ", "") + "/" + String.valueOf(nonce));  // saves as solution/nonce
			zooKeeperCommunicator.putNonce(nonce);
			solution_found = true;
		    }
		    
		} while (interval_iterations < INTERVAL_ITERATIONS);
	    	
		// check to see if our problem hash changed, and reset

		config = zooKeeperCommunicator.getConfig();

		if (config == null) break;         // master program want's us to shut down

		String new_block_header  = config.split("/")[0];
		String new_target_header = config.split("/")[1];

		// we've been assigned a new problem:

		if (! new_block_header.equals(block_header))  {

		    solution_found = false;
		    block_header   = new_block_header;

		    digest = initializeSha256(block_header);
		    target = hexStringToBytesWritable(new_target_header);
		    nonce  = start_nonce;   
		}

		// update our stats if we're still working on a problem

		if (! solution_found) {
		    zooKeeperCommunicator.putNonce(nonce);
		}
	    }
	}
    }

    // Essentially the identity reduction - if we have multiple values, it's an error.  Every map task should be getting its own nonce.
    // Produces the <final-nonce,elapsed-time> pairs for checking performance.
	
    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
	public void reduce(LongWritable key, Iterator<IntWritable> values, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException {
	    output.collect(key, new IntWritable(values.next().get()));
	}
    }


    // arguments:
    // DFS input directory, DFS output directory, number of map tasks
 
    public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf(BitMine.class);

	conf.setJobName("BitCoin Miner");
	conf.setBoolean("mapred.output.compress", false);

	conf.setNumMapTasks(Integer.parseInt(args[2]));
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
	

    // Convert a long to an array of bytes.

    private static byte[] longToBytes(long number) {
	int len;

	if (number == 0) {
	    len = 1;
	} else {
	    len = (int) Math.ceil(Math.log10(number) / Math.log10(256) + 0.000000001);    // get number of places base 256
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

