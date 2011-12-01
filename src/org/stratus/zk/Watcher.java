package org.stratus.zk;

// This class watches the zookeeper directories, looking for solutions
// and displaying continuous statistics. main() takes one argument, 
// the host address of the zookeeper server to connect to (e.g., localhost:2181).
//
// Worker processes are maintain  nodes under /workers.
// If a worker process' name is bc-001, say, then there will be
//
//    /workers/bc-001/active
//    /workers/bc-001/current-nonce
//    /workers/bc-001/initial-nonce
//    /workers/bc-001/solution
//
// The znode /workers/bc-001/active is ephermeral and will not exist 
// if the worker process has exited. We ignore the dead processes
//
// 

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

public class MasterSetup implements Watcher {
  
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

    // remove traces of dead workers: /workers/bc-000000000*/{solution,initial-nonce,current-nonce}
  
    public void cleanUpWorkers() throws KeeperException,  InterruptedException {
	Stat stat;
	String path;

	try {
	    List<String> children = zk.getChildren("/workers", false);
	    for (String child : children) {
		path = "/workers/" + child;
		stat = zk.exists(path + "/active", false);
		if (stat == null) {
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

    public static void main(String[] args) throws Exception {
	MasterSetup masterSetup = new MasterSetup();
	// args:  "host:port",  "configdata"
    
	masterSetup.connect(args[0]);
	masterSetup.createConfigNode(args[1]);
	masterSetup.createWorkersNode();
	masterSetup.cleanUpWorkers();
	masterSetup.close();
    }
}

