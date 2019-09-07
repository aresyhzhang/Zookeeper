package com.zyh.zookeeper.test;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZKTest {
	private String connectString="hadoop120:2181,hadoop121:2181,hadoop122:2181";
	//超时时间必须介于2*minsessionTimeout和20*minsessionTimeout
	private int sessionTimeout=30000;
	
	private ZooKeeper zookeeper;
	
	
	@Before
	public void testConnect() throws Exception {
		//shell : zkCli.sh -server host:port
		//连接
	//创建客户端对象
	 zookeeper=new ZooKeeper(connectString, sessionTimeout, new Watcher() {
		//process会在结点变化后，由watcher进行调用
		@Override
		public void process(WatchedEvent arg0) {
			// TODO Auto-generated method stub
			System.out.println("当前调用了这个观察者");
		}
	});
	}
	
	//增：create [-s ] [-e] path data acl
	@Test
	public void createNodes() throws Exception {
		String path=zookeeper.create("/eclipse","hello".getBytes() , Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(path+"has been created！");		
	}
	
	//删 delete path,只能删除空结点
	@Test
	public void delNodes() throws Exception {
		//version 代表cversion,-1代表忽略版本号
		zookeeper.delete("/eclipse/node1", 0);
	}
	
	//改 set path data
	@Test
	public void setData () throws Exception {
	     zookeeper.setData("/eclipse","heihei".getBytes() , -1);
	}
	
	//查：遍历子节点 ls path
	@Test
	public void listPath() throws Exception {
		List<String> children = zookeeper.getChildren("/", new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getPath()+":发生了事件变化如下"+event.getType());
				
			}
		});
		System.out.println(children);
		while(true) {
			Thread.sleep(5000);
			System.out.println("我还活着");
		}
		}
	
	//查： 查询结点的数据 get path 阻塞了process线程 无法完成持续监听
	@Test
	public void getData() throws Exception {
//		byte[] data = zookeeper.getData("/eclipse", false, null);
//		System.out.println(new String(data));
     byte[] data = zookeeper.getData("/eclipse", new Watcher() {			
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getPath()+":发生了事件变化如下"+event.getType());	
				try {
					 getData();					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}, null);
		System.out.println("当前:"+"的值是："+new String(data));
		while(true) {
			Thread.sleep(5000);
			System.out.println("我还活着");
		}
	}
	
	//查：判断当前结点是否存在
	@Test
	public void isExists () throws Exception {
		Stat stat = zookeeper.exists("/eclipse", false);
		if (stat==null) {
			System.out.println("结点不存在");
		}else {
			System.out.println(stat);
		
		}
	}
	
	//持续监听 核心就是在process（）中递归调用，重新设置监听
	public String getData2(String path) throws Exception {
		byte[] data = zookeeper.getData(path, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getPath()+":发生了事件变化如下"+event.getType());	
				try {
					String newData = getData2(path);
					System.out.println("变化后的数据是："+newData);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}, null);
		System.out.println("当前:"+path+"的值是："+new String(data));
		return new String(data);
	}
	
	@Test
	public void testWatchAlways() throws Exception {
		String data = getData2("/eclipse");
		System.out.println("当前结点查询的数据是："+data);
		while(true) {
			Thread.sleep(5000);
			System.out.println("我还活着");
		}
	}
	
	
	@After
	public void close() throws InterruptedException {
		if (zookeeper!=null) {
			zookeeper.close();			
		}
	}
}
