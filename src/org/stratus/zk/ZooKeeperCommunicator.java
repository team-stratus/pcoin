package org.stratus.zk;

import java.nio.charset.Charset;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;


public class ZooKeeperCommunicator extends ConnectionWatcher {

    private static final Charset CHARSET = Charset.forName("UTF-8");

    public  String node_name;
    private String initial_nonce;
    private String current_nonce;
    private String solution;

    //  Establish a node for ourselves under /workers.
    //
    //  We add the following children to our newly-created node:
    //
    //     /initial-nonce, for our start nonce
    //     /current-nonce, for occasional updates of where we are
    //     /solution,  to put our solution, if we find one
    //     /active, an ephemeral node which will disappear if we exit, so the master knows which workers to check, which to remove
  
    public void join(String nonce) throws KeeperException, InterruptedException {
	String path = "/workers/bc-";
	byte[] nonce_data = nonce.getBytes(CHARSET);

	node_name = zk.create("/workers/bc-",   null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

	initial_nonce = zk.create(node_name + "/initial-nonce", nonce_data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	current_nonce = zk.create(node_name + "/current-nonce", nonce_data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	solution  = zk.create(node_name + "/solution", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	zk.create(node_name + "/active", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }
  
    // get the bitcoin mining problem

    public String getConfig() throws IOException {
	try {
	    return new String(zk.getData("/config", false, null), CHARSET);
	} catch (Exception e) {
	    throw new IOException("can't get config from zoo keeper"); 
	}	    
    }

    // every once in a while we write the nonce we are using to the current-nonce node

    public void putNonce(long nonce) throws IOException {
	try {
	    Stat stat = zk.exists(current_nonce, false);
	    if (stat != null) {
		zk.setData(current_nonce, String.valueOf(nonce).getBytes(CHARSET), -1);
	    }
	} catch (Exception e) {
	    throw new IOException("can't put nonce to zoo keeper"); 
	}
    }

    // if we find a solution, let's post it. a monitor program will
    // grab it and update the /config node with the next problem.

    public void putSolution(String solution) throws IOException {
	try {
	    Stat stat = zk.exists(solution, false);
	    if (stat == null) {
		solution  = zk.create(node_name + "/solution", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	    }
	    zk.setData(solution, solution.getBytes(CHARSET), -1);
	} catch (Exception e) {
	    throw new IOException("can't put solution to zoo keeper"); 
	}
    }


    public static ZooKeeperCommunicator setUp(String host_address, long nonce) throws IOException {
	ZooKeeperCommunicator zooKeeperCommunicator = new ZooKeeperCommunicator();
	try {
	    zooKeeperCommunicator.connect(host_address);
	    zooKeeperCommunicator.join(String.valueOf(nonce));
	    return zooKeeperCommunicator;

	} catch (Exception e) {
	    throw new IOException("can't setup communications channel with zoo keeper"); 
	}
    }



    public static void main(String[] args) throws Exception {

	// args: "hostname:port", nonce

	String host_address  = args[0];
	String initial_nonce = args[1];

	// ZooKeeperCommunicator zooKeeperCommunicator = new ZooKeeperCommunicator();

	ZooKeeperCommunicator zooKeeperCommunicator = ZooKeeperCommunicator.setUp(host_address, Long.parseLong(initial_nonce));

    
	String config = zooKeeperCommunicator.getConfig();

	System.out.println("Assigned node name is: " + zooKeeperCommunicator.node_name);
	System.out.println("Initial config is: " + config);
	System.out.println("Initial nonce is: " + initial_nonce);

	// hang out for a while

	String current_config = config;

	long sleep_interval = 1000 * 15;
	long nonce = Long.parseLong(initial_nonce);
	
	while (true) {
	    Thread.sleep(sleep_interval);
	    config = zooKeeperCommunicator.getConfig();
	    if (! current_config.equals(config)) {
		current_config = config;
		System.out.printf("Config has changed for worker %s to  '%s'\n", zooKeeperCommunicator.node_name, current_config);
		System.out.printf("Resetting nonce to %s from current %d\n", initial_nonce, nonce);
		nonce = Long.parseLong(initial_nonce);
	    }
	    nonce = nonce + sleep_interval;
	    zooKeeperCommunicator.putNonce(nonce);
	}
    }
}

