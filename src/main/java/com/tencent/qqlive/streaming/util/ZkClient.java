package com.tencent.qqlive.streaming.util;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;

public class ZkClient implements Watcher {
	private static final int SESSION_TIMEOUT = 30000;
	
	private final CountDownLatch latch = new CountDownLatch(1);
	private ZooKeeper zk = null;
	
	private String zkHost = null;
	
	public ZkClient(String zkHost) {
		this.zkHost = zkHost;
	}
	
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			latch.countDown();
		}		
	}
	
	public void connect() throws Exception {
		zk = new ZooKeeper(zkHost, SESSION_TIMEOUT, this);
		latch.await();
	}
	
	public void register(String path, String address, int port) throws Exception {
		String taskPath = path + "/" + address + "-" + port;
		Stat s = zk.exists(taskPath, false);
		if (s == null) {
			zk.create(taskPath, nodeData(address, port).getBytes(), 
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} else {
			throw new RuntimeException("zkpath exists:" + taskPath);
		}		
	}
	
	public void writeConf(String path, String conf) throws Exception {
		Stat s = zk.exists(path, false);
		if (s != null) {
			zk.delete(path, s.getVersion());
		}
		
		zk.create(path, conf.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}
	
	public String readConf(String path) throws Exception {
		Stat s = zk.exists(path, false);
		if (s == null) 
			throw new RuntimeException("node does not exists: " + path);
		
		byte[] data = zk.getData(path, false, s);
		
		return new String(data);
	}
	
	public void disconnect() throws Exception {
		zk.close();
	}
	
	private String nodeData(String address, int port) {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		
		sb.append("\"");
		sb.append("address");
		sb.append("\"");
		sb.append(":");
		sb.append("\"");
		sb.append(address);
		sb.append("\"");
		sb.append(",");
		sb.append("\"");
		sb.append("port");
		sb.append("\"");
		sb.append(":");
		sb.append("\"");
		sb.append(port);
		sb.append("\"");
		
		sb.append("}");
		
		return sb.toString();
	}
}
