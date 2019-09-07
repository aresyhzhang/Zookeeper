package com.zyh.zookeeper.example;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

//每个server进程启动时，向指定的znode目录，创建临时结点，保存当前服务的信息到这个结点
public class Server {
		private String connectString="hadoop120:2181,hadoop121:2181,hadoop122:2181";
		private int sessionTimeout=30000;
		private ZooKeeper zookeeper;
		private String parentPath="/Servers";
		public void init() throws Exception{
			 zookeeper=new ZooKeeper(connectString, sessionTimeout, new Watcher() {
					//process会在结点变化后，由watcher进行调用
					@Override
					public void process(WatchedEvent arg0) {
						// TODO Auto-generated method stub
						System.out.println("当前调用了这个观察者");
					}
				});	
		}
		
		public void validParentPath() throws Exception, InterruptedException {
			Stat stat = zookeeper.exists(parentPath, false);
			if (stat==null) {
				System.out.println("结点不存在");
				String createdPath=zookeeper.create(parentPath, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				System.out.println(createdPath+"has been created");
			}else {
				System.out.println(stat);
			
			}
		}
		
		public void regist(String data) throws  Exception {
			zookeeper.create(parentPath+"/"+"server", data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		}
		
		public void doServer() throws InterruptedException {
			System.out.println("当前进程正在提供服务...");
			while(true) {
				Thread.sleep(Integer.MAX_VALUE);
			}
		}
		
		public static void main(String[] args) throws Exception {
			Server server = new Server();
			//1.创建Zookeeper客户端
			server.init();
			
			//2.验证指定目录是否存在，初始化
			server.validParentPath();
			
			//3.注册服务
			server.regist(args[0]);
			
			//4.运行自己的业务代码
			server.doServer();
			
		}
}
