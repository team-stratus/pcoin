package org.stratus.zk;

import java.nio.charset.Charset;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;
import java.io.IOException;


public class WorkerSetup extends ConnectionWatcher {

    private static final Charset CHARSET = Charset.forName("UTF-8");

    public  String node_name;
    private String initial_nonce;
    private String current_nonce;
    private String solution;

    //  Establish a node for ourselves under /workers.
    //
    //  We add: 
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
  
    public String getConfig() throws KeeperException, InterruptedException {
	return new String(zk.getData("/config", false, null), CHARSET);
    }

    public void putNonce(String nonce) throws KeeperException, InterruptedException {
	Stat stat = zk.exists(current_nonce, false);
	if (stat != null) {
	    zk.setData(current_nonce, nonce.getBytes(CHARSET), -1);
	}
    }

    public void putSolution(String solution) throws KeeperException, InterruptedException {
	Stat stat = zk.exists(solution, false);
	if (stat != null) {
	    zk.setData(solution, solution.getBytes(CHARSET), -1);
	}
    }

    public static WorkerSetup setUp(String host_address, long nonce) throws IOException {
    	try {

    	    WorkerSetup ws = new WorkerSetup();
    	    ws.connect(host_address);
    	    ws.join(String.valueOf(nonce));
    	    return ws;

    	} catch (Exception e) {
    	    throw new IOException("can't setup communications channel with zoo keeper"); 
    	}
    }


    public static void main(String[] args) throws Exception {
	// WorkerSetup workerSetup = new WorkerSetup();

	// args: "hostname:port", nonce

	String host_address  = args[0];
	String initial_nonce = args[1];
	
	// WorkerSetup workerSetup = new WorkerSetup();
	// workerSetup.connect(host_address);
	// workerSetup.join(initial_nonce);

    	WorkerSetup workerSetup = WorkerSetup.setUp(host_address, Long.parseLong(initial_nonce));

	String config = workerSetup.getConfig();

	System.out.println("Assigned node name is: " + workerSetup.node_name);
	System.out.println("Initial config is: " + config);
	System.out.println("Initial nonce is: " + initial_nonce);

	// hang out for a while

	String current_config = config;

	long sleep_interval = 1000 * 15;
	long nonce = Long.parseLong(initial_nonce);
	
	while (true) {
	    Thread.sleep(sleep_interval);
	    config = workerSetup.getConfig();
	    if (! current_config.equals(config)) {
		current_config = config;
		System.out.printf("Config has changed for worker %s to  '%s'\n", workerSetup.node_name, current_config);
		System.out.printf("Resetting nonce to %s from current %d\n", initial_nonce, nonce);
		nonce = Long.parseLong(initial_nonce);
	    }
	    nonce = nonce + sleep_interval;
	    workerSetup.putNonce(String.format("%d", nonce));
	}
    }
}

