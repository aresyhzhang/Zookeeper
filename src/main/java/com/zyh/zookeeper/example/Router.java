package com.zyh.zookeeper.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class Router {
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
	
	public  List<String> getDatas() throws KeeperException, InterruptedException {
		ArrayList<String> datas = new ArrayList<>();
	    List<String> children = zookeeper.getChildren(parentPath, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getPath()+":发生了事件变化如下"+event.getType());	
				List<String> newDatas;
				try {
					newDatas = getDatas();
					System.out.println("新的可用的结点是"+newDatas);					
				} catch (KeeperException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
				
			}
		});
	    
	    for (String string : children) {
			byte[] data = zookeeper.getData(parentPath+"/"+string, false, null);
			datas.add(new String(data));
		}
	    return datas;	
	}
	
	public void doServer() throws InterruptedException {
		System.out.println("当前进程正在提供服务...");
		while(true) {
			Thread.sleep(Integer.MAX_VALUE);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Router router=new Router();
		//1.创建zookeeper客户端
		router.init();
		
		//2.持续监听，遍历指定目录，获取当前运行的线程及其他信息
		List<String> datas = router.getDatas();
		System.out.println("当前获取到的信息是"+datas);
		
		//3.运行自己的业务代码
		router.doServer();
		
	}
}
