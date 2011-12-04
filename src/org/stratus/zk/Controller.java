package org.stratus.zk;

// This class is a utility to control zookeeper configuration data for
// map/reduce bitcoin operations.
//
// We can use it to initialize, shudown, cleanup, and report on the
// state of a bitcoin mining session.
//
//  usage:
//          java Controller host:port subcommand more args...
//
//  subcomands:
//
//     initialize config-string
//     cleanup
//     shutdown
//     report
//



// initialize:
//
// sets up the zookeeper directories: /config and /workers. Requires
// two additional arguments, the host address of the zookeeper server
// to connect to (e.g., localhost:2181) and the initial bitcoin mining
// problem. /config gets set to the value of the latter.
//
// Worker processes are responsible for setting up nodes under /workers.
// If a worker process' name is bc-001, say, then there will be
//
//    /workers/bc-001/active
//    /workers/bc-001/current-nonce
//    /workers/bc-001/initial-nonce
//    /workers/bc-001/solution
//

// cleanup:
//
// The znode /workers/bc-001/active is ephermeral and will not exist 
// if the worker process has exited.  We take advantage of this to 
// remove any extinct workers on cleanup, but leave active ones alone.

// shutdown:
//
// removes the /config znode;  worker tasks are designed to immediately exit 


import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Controller implements Watcher {
  
    private static final int SESSION_TIMEOUT = 5000;
    private static final Charset CHARSET = Charset.forName("UTF-8");

    private ZooKeeper zk;
    private CountDownLatch connectedSignal = new CountDownLatch(1);
  
    public void connect(String hosts) throws IOException, InterruptedException {
	zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
	connectedSignal.await();
    }
  
    // watcher interface: really only used on initial connection, everything else here blocks

    @Override
	public void process(WatchedEvent event) {
	if (event.getState() == KeeperState.SyncConnected) {
	    connectedSignal.countDown();
	}
    }


    // create the znode /config if it doesn't already exist
    
    public void createConfigNode(String configData) throws KeeperException,  InterruptedException {
	Stat stat = zk.exists("/config", false);
       	if (stat == null)  {
	    String createdPath = zk.create("/config", configData.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	    System.out.println("Created " + createdPath);
	} else {
	    zk.setData("/config", configData.getBytes(CHARSET), -1);
	}
    }

    // create the znode /workers if it doesn't already exist

    public void createWorkersNode() throws KeeperException,  InterruptedException {
	Stat stat = zk.exists("/workers", false);
       	if (stat == null)  {
	    String createdPath = zk.create("/workers", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	    System.out.println("Created " + createdPath);
	}
    }

    // delete the znode /config which indicates to the map-reduce tasks that they should exit

    public void removeConfigNode() throws KeeperException,  InterruptedException {
	Stat stat = zk.exists("/config", false);
       	if (stat == null)  {
	    System.out.println("/config node has already been deleted");
	    return;
	} else {
	    zk.delete("/config", -1);
	    System.out.println("Deleted /config node");
	}
    }


    // remove traces of dead workers: /workers/bc-000000000*/{solution,initial-nonce,current-nonce,active}
    // active is ephemeral and won't exist if the task has exited
  
    public void cleanUpWorkers() throws KeeperException,  InterruptedException {
	Stat stat;
	String path;

	try {
	    List<String> children = zk.getChildren("/workers", false);
	    for (String child : children) {
		path = "/workers/" + child;
		stat = zk.exists(path + "/active", false);
		if (stat == null) {
		    System.out.println("Removing defunct task " + path);
		    zk.delete(path + "/solution", -1);
		    zk.delete(path + "/initial-nonce", -1);
		    zk.delete(path + "/current-nonce", -1);
		    zk.delete(path, -1);
		}
	    }
	} catch (KeeperException.NoNodeException e) {
	    System.out.println("warning: cleanup issue: check children of znode /workers");
	}
    }

  
    public void close() throws InterruptedException {
	zk.close();
    }

    public static void usage() {
	System.err.println("usage:");
	System.err.println(" java Controller host:port subcommand [subcommand args]");
	System.err.println(" where subcommand is one of 'initialize', 'cleanup', 'shutdown', 'report'");
	System.err.println(" initialize <string>");
	System.err.println("   takes one argument, a problem, and stores it to the '/config' znode");
	System.err.println(" cleanup");
	System.err.println("   removes completed worker tasks from the children of the '/workers' znode");
	System.err.println(" shutdown");
	System.err.println("   removes the '/config' znode, which signals the worker tasks to exit");
	System.err.println(" report");
	System.err.println("   lists a summary of the zookeeper directory, including timing information");
	System.exit(-1);
    }
    

    public void report() throws KeeperException,  InterruptedException {
	String path;
	String config_data;
	Stat stat;

	stat = zk.exists("/config", false);
	
	if (stat != null) {
	    config_data = new String(zk.getData("/config", false, null), CHARSET);
	} else {
	    config_data = null;
	}

	if (stat == null) {
	    System.out.println("Config: not found (system has been shutdown)");
	} else if (config_data == null) {
	    System.out.println("Config: no data");
	} else {
	    System.out.println("Config: " + config_data);
	}
	
	List<String> children = zk.getChildren("/workers", false);
	for (String child : children) {

	    path = "/workers/" + child;	    
	    
	    String start_nonce = "not available";

	    stat = zk.exists(path + "/initial-nonce", false);
	    if (stat != null) {
		start_nonce = new String(zk.getData(path + "/initial-nonce", false, null), CHARSET);
	    }

	    stat = zk.exists(path + "/active", false);

	    if (stat == null) {
		System.out.println("  " + path + " (" + start_nonce + ")"); 
	    } else {
		System.out.println("* " + path + " (" + start_nonce + ")"); 
	    }
	    
	    long initial_nonce = 0;
	    long final_nonce   = 0;
	    long millisecs     = 0;

	    Stat istat = null;
	    Stat cstat = null;
	    Stat sstat = null;

	    istat = zk.exists(path + "/initial-nonce", false);

	    if (istat != null) {
		String str = new String(zk.getData(path + "/initial-nonce", false, null), CHARSET);
		initial_nonce = Long.parseLong(str);
	    } else {
		System.err.println("Can't get initial-nonce for timing data");
	    }

	    cstat = zk.exists(path + "/current-nonce", false);

	    if (cstat != null) {
		String str = new String(zk.getData(path + "/current-nonce", false, null), CHARSET);
		final_nonce = Long.parseLong(str);
		millisecs = cstat.getMtime() - cstat.getCtime();
	    } else {
		System.err.println("Can't get current-nonce for timing data");
	    }
	    
	    String soln = null;

	    sstat = zk.exists(path + "/solution", false);

	    if (sstat != null) {
		byte[] data;
		data = zk.getData(path + "/solution", false, null);
		if (data != null) {
		    soln = new String(data, CHARSET);
		} else {
		    soln = null;
		} 
	    }

	    if (soln != null && cstat != null && istat != null) {
		System.out.printf("     solution: %s\n", soln);		
		System.out.printf("     %3.2f trials/sec\n", 1000.0 * (final_nonce - initial_nonce)  / millisecs);
	    } else if (cstat != null && istat != null) {
		System.out.printf("     %3.2f trials/sec\n", 1000.0 * (final_nonce - initial_nonce)  / millisecs);
	    } else {
		System.out.println("");
	    }
	}	
    }


    // entry point;  switches to one of the following sub-commands
    //
    //    cleanup                   - remove records of exited tasks from zookeeper directory
    //    initialize data-string    - initialize the bitcoin problem with data-string
    //    report                    - show some statistics for, and states of, the bitcoin tasks
    //    shutdown                  - indicate that the tasks should exit

    public static void main(String[] args) throws Exception {
	Controller controller = new Controller();

	// args:  "host:port",  "configdata"

	if (args.length < 2) {
	    usage();
	}
    
	controller.connect(args[0]);
	
	String cmd = args[1];

	if (cmd.equals("init") || cmd.equals("initialize")) {
	    controller.createConfigNode(args[2]);
	    controller.createWorkersNode();

	} else if (cmd.equals("clean") || cmd.equals("cleanup")) {
	    controller.cleanUpWorkers();

	} else if (cmd.equals("stop") || cmd.equals("shutdown")) {
	    controller.removeConfigNode();

	} else if (cmd.equals("report")) {
	    controller.report();
	} else {
	    usage();
	}

	controller.close();
    }
}

