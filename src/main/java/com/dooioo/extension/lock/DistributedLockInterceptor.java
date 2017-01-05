package com.dooioo.extension.lock;

import com.dooioo.cluster.ClusterConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.concurrent.locks.Lock;

/**
 * 分布式锁拦截器
 * <p> 注意 此处必须定义Order顺序 且value必须小于tx Order(已设置tx order = '1' 默认不设置为0) 以保证在tx之前运行，
 * 否则事务创建后若分布式锁等待时间过久，必导致事务超时</p>
 * @author dingxin
 *
 */
public class DistributedLockInterceptor{
	
	private static final Log logger = LogFactory.getLog(DistributedLockInterceptor.class);

	private String zookeeperConfig = ClusterConfiguration.getInstance().getZookeeperConfig();
	private String appCode = ClusterConfiguration.getInstance().getAppCode();
	
	
	public DistributedLockInterceptor() {
		if(appCode == null)
			throw new DistributedLockException("appCode must not be null! Can't init DistributedLockInterceptor!");
		logger.info("检查zookeeper根目录！");
		ZooKeeper zk = null;
		try {
			zk = new ZooKeeper(zookeeperConfig, 3000, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					logger.info("连接zookeeper服务器！");
				}
			});
			Stat stat = zk.exists("/locks", false);
			if(stat == null){
				logger.info("初始化分布式锁根目录");
				zk.create("/locks", "分布式锁根目录".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			}
			Stat appStat = zk.exists("/locks/"+appCode, false);
			if(appStat == null){
				logger.info("初始化["+appCode+"]分布式锁目录");
				zk.create("/locks/"+appCode, ("["+appCode+"]分布式锁目录").getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			}
			logger.info("检查zookeeper根目录完毕！");
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		} catch (KeeperException e) {
			logger.error(e.getMessage(), e);
		} finally {
			if(zk != null) {
				try {
					zk.close();
				} catch (InterruptedException e) {
				}
			}
		}
		logger.info("DistributedLockInterceptor init successfully!");
	}

	public Object Around(ProceedingJoinPoint joinPoint) throws Throwable{
		Signature signature = joinPoint.getSignature();
    	Method method = ((MethodSignature)signature).getMethod();
    	DistributedSynchronized sync = method.getAnnotation(DistributedSynchronized.class);
		String lockName = sync.value();
		if(StringUtils.isEmpty(lockName) || lockName.contains(DistributedLock.splitStr))
			lockName = method.getDeclaringClass().getName()+"."+method.getName();
		Lock distributedLock = new DistributedLock(appCode, zookeeperConfig, lockName);
		distributedLock.lock();
		Object retObj = null;
		try {
			retObj = joinPoint.proceed();
		} catch (Exception e) {
			throw e;
		} finally {
			distributedLock.unlock();
		}
		return retObj;
	}

}

