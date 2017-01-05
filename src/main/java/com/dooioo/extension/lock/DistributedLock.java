package com.dooioo.extension.lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.regex.Pattern;


/**
 * <p>基于zookeeper的分布式锁
 * </p>
 * plz make sure that lockTimeout less than sessionTimeout
 */
public class DistributedLock implements Lock, Watcher{

	private static final Log logger = LogFactory.getLog(DistributedLock.class);

	private static final String localIp;

	static{
		Set<String> set = getLocalIps();
		localIp = set.stream().filter(ip -> !"127.0.0.1".equals(ip)).findFirst().get();
	}

	private ZooKeeper zk;
	private String root = "/locks";//根
	private final String lockName;//竞争资源的标志
	private String waitNode;//等待前一个锁
	private String myZnode;//当前锁
	private CountDownLatch latch;//计数器
	private static final int DEFAULT_SESSION_TIMEOUT = 240000; //ms
	private static final int DEFAULT_LOCK_TIMEOUT = 180000;  //ms
	private List<Exception> exception = new ArrayList<Exception>();
	public static final String splitStr = "_dooioo_distributed_lock_";


    /**
     * 创建分布式锁,使用前请确认config配置的zookeeper服务可用
     * @param config 127.0.0.1:4180
     * @param lockName 竞争资源标志,lockName中不能包含单词{@code _dooioo_distributed_lock_}
     */
    public DistributedLock(String appCode, String config, String lockName){
        this(appCode, config, lockName, DEFAULT_SESSION_TIMEOUT);
    }

	/**
	 * 创建分布式锁,使用前请确认config配置的zookeeper服务可用
	 * @param config 127.0.0.1:4180
	 * @param lockName 竞争资源标志,lockName中不能包含单词{@code _dooioo_distributed_lock_}
     * @param sessionTimeout 超时时间 ms
	 */
	public DistributedLock(String appCode, String config, String lockName, int sessionTimeout){
		this.lockName = lockName;
		root += "/" + appCode;
		// 创建一个与服务器的连接
		 try {
			zk = new ZooKeeper(config, sessionTimeout, this);
		} catch (IOException e) {
			if(logger.isErrorEnabled())
				logger.error(e.getMessage(),e);
			exception.add(e);
		}
	}

	/**
	 * zookeeper节点的监视器
	 */
	@Override
	public void process(WatchedEvent event) {
		if(this.latch != null && waitNode != null && this.latch.getCount() == 1) {
			if(event.getType().equals(EventType.NodeDeleted) && (root+"/"+waitNode).equals(event.getPath())){
				this.latch.countDown();
			}
        }
	}

	public void lock() {
		if(exception.size() > 0){
			try {
				if(myZnode != null && zk != null){
					zk.delete(myZnode, -1);
					myZnode = null;
				}
			} catch (Exception e) {
			}
			throw new LockException(exception.get(0));
		}
		try {
			if(this.tryLock()){
				if(logger.isDebugEnabled())
					logger.debug("Thread " + Thread.currentThread().getId() + " " +myZnode + " get lock!");
				return;
			}
			else{
				waitForLock(waitNode, DEFAULT_LOCK_TIMEOUT, TimeUnit.MILLISECONDS);//等待锁
			}
		} catch (KeeperException e) {
			try {
				if(myZnode != null && zk != null){
					zk.delete(myZnode, -1);
					myZnode = null;
				}
			} catch (Exception e2) {
			}
			try {
				zk.close();
			} catch (InterruptedException e1) {
			}
			throw new LockException("Thread is "+Thread.currentThread().getId()+" lockName is "+lockName+" myZnode is "+myZnode+" waitNode is "+waitNode+" "+e.getMessage(),e);
		} catch (InterruptedException e) {
			try {
				if(myZnode != null && zk != null){
					zk.delete(myZnode, -1);
					myZnode = null;
				}
			} catch (Exception e2) {
			}
			try {
				zk.close();
			} catch (InterruptedException e1) {
			}
			throw new LockException("Thread is "+Thread.currentThread().getId()+" lockName is "+lockName+" myZnode is "+myZnode+" waitNode is "+waitNode+" "+e.getMessage(),e);
		}
	}

	public boolean tryLock() {
		try {
			if(lockName.contains(splitStr))
				throw new LockException("lockName can not contains ［"+splitStr+"］");
			//创建临时子节点
			myZnode = zk.create(root + "/" + lockName + splitStr, (Thread.currentThread().getId() + "|" + localIp + "|" + System.currentTimeMillis()).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
			if(logger.isDebugEnabled())
				logger.debug(myZnode + " is created");
			//取出所有子节点
			List<String> subNodes = zk.getChildren(root, false);
			//取出所有lockName的锁
			List<String> lockObjNodes = new ArrayList<String>();
			for (String node : subNodes) {
				String _node = node.split(splitStr)[0];
				if(_node.equals(lockName)){
					lockObjNodes.add(node);
				}
			}
			Collections.sort(lockObjNodes);
			if(logger.isDebugEnabled())
				logger.debug(myZnode + "==" + lockObjNodes.get(0));
			if(myZnode.equals(root+"/"+lockObjNodes.get(0))){
				//如果是最小的节点,则表示取得锁
	            return true;
	        }
			//如果不是最小的节点，找到比自己小1的节点
			String subMyZnode = myZnode.substring(myZnode.lastIndexOf("/") + 1);
			waitNode = lockObjNodes.get(Collections.binarySearch(lockObjNodes, subMyZnode) - 1);
		} catch (KeeperException e) {
			try {
				if(myZnode != null && zk != null){
					zk.delete(myZnode, -1);
					myZnode = null;
				}
			} catch (Exception e2) {
			}
			throw new LockException(e);
		} catch (InterruptedException e) {
			try {
				if(myZnode != null && zk != null){
					zk.delete(myZnode, -1);
					myZnode = null;
				}
			} catch (Exception e2) {
			}
			throw new LockException(e);
		}
		return false;
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) {
		try {
			if(this.tryLock()){
				return true;
			}
	        return waitForLock(waitNode,time, unit);
		} catch (Exception e) {
			if(logger.isErrorEnabled())
				logger.error(e.getMessage(),e);
		}
		return false;
	}

	private boolean waitForLock(String lower, long waitTime, TimeUnit unit) throws InterruptedException, KeeperException {
        //判断比自己小一个数的节点是否存在,如果不存在则无需等待锁,同时注册监听
		//此处有风险 latch 必须先于 watch 初始化 否则高并发下 可能刚注册完watch latch 被初始化之前watch 就被触发 锁无法得到释放 直至超时
		this.latch = new CountDownLatch(1);
        Stat stat = zk.exists(root + "/" + lower,this);
        if(stat != null){
        	if(logger.isDebugEnabled())
        		logger.debug("Thread " + Thread.currentThread().getId() + " waiting for " + root + "/" + lower);
        	this.latch.await(waitTime, unit);
        }
        this.latch = null;
        if(logger.isDebugEnabled())
			logger.debug("Thread " + Thread.currentThread().getId() + " " +myZnode + " get lock!");
        return true;
    }

	public void unlock() {
		try {
			if(logger.isDebugEnabled())
				logger.debug("unlock " + myZnode);
			zk.delete(myZnode,-1);
			myZnode = null;
			zk.close();
		} catch (InterruptedException e) {
			if(logger.isWarnEnabled())
				logger.warn(e.getMessage(), e);
		} catch (KeeperException e) {
			if(logger.isWarnEnabled())
				logger.warn(e.getMessage(), e);
		}
	}

	public void lockInterruptibly() throws InterruptedException {
		this.lock();
	}

	public Condition newCondition() {
		//不支持线程间通信
		throw new UnsupportedOperationException();
	}

	public class LockException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		public LockException(String e){
			super(e);
		}
		public LockException(Exception e){
			super(e);
		}
		public LockException(String message, Throwable cause) {
			super(message, cause);
		}

	}

	 private static Set<String> getLocalIps() {
	        Set<String> ips = new HashSet<String>();
	        Pattern pattern = Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
	        try {
	            Enumeration<NetworkInterface> networkList = NetworkInterface.getNetworkInterfaces();
	            while (networkList.hasMoreElements()) {
	                Enumeration<InetAddress> list = networkList.nextElement().getInetAddresses();
	                while (list.hasMoreElements()) {
	                    String ip = list.nextElement().getHostAddress();
	                    if (pattern.matcher(ip).matches()) {
	                    	ips.add(ip);
	                    }
	                }
	            }
	        } catch (SocketException e) {
	        	if(logger.isWarnEnabled())
					logger.warn(e.getMessage(), e);
	        }
	        return ips;
	    }

}