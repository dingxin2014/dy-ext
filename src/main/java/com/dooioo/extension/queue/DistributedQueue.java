package com.dooioo.extension.queue;

import com.dooioo.cluster.ClusterConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.util.CollectionUtils;

import javax.security.auth.login.Configuration;
import java.io.*;
import java.text.NumberFormat;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * <p>
 * 分布式队列
 *  节点A:
 * <pre>    DistributedQueue queue = new DistributedQueue<String>("appCode","testQueue");
 *  queue.add("test");
 * </pre>
 *  则节点B:
 * <pre>    DistributedQueue queue = new DistributedQueue<String>("appCode","testQueue");
 *  for(String s: queue){
 *      System.out.println(s);
 *  }
 * </pre>
 * <pre>
 *  控制台输出 test
 * </pre>
 * <b>
 *  分布式线程安全
 * </b>
 *  e.g1:对于节点A 和 节点B 同时poll();则必定有一个节点返回null
 *  e.g2:遍历 新增 移除可以并发进行
 * </p>
 *
 * Created by dingxin on 16/12/7.
 */
public class DistributedQueue<E> implements Queue<E>, Serializable {

    private static final Log logger = LogFactory.getLog(DistributedQueue.class);

    private static final String zookeeperConfig = ClusterConfiguration.getInstance().getZookeeperConfig();

    private static final String appCode = ClusterConfiguration.getInstance().getAppCode();

    private static final String root = "/queue";

    static{
        logger.info("检查[zookeeper queue]根目录！");
        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper(zookeeperConfig, 3000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    logger.info("连接zookeeper服务器！");
                }
            });
            Stat stat = zk.exists(root, false);
            if(stat == null){
                logger.info("初始化分布式锁根目录");
                zk.create(root, "分布式锁根目录".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
            Stat appStat = zk.exists(root+"/"+appCode, false);
            if(appStat == null){
                logger.info("初始化["+appCode+"]分布式锁目录");
                zk.create(root+"/"+appCode, ("["+appCode+"]分布式锁目录").getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
            logger.info("检查[zookeeper queue]根目录完毕！");
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
    }

    private ZooKeeper zk;

    private String queueName;
    private String path;
    private int sessionTimeout = 300000;


    public DistributedQueue(String queueName){
        this(appCode, queueName);
    }

    public DistributedQueue(String appCode, String queueName){
        this.queueName = queueName;
        this.path = root + "/" + appCode + "/" + queueName;
        try {
            this.zk = new ZooKeeper(zookeeperConfig, sessionTimeout, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                }
            });
            Stat stat = zk.exists(this.path, false);
            if(stat == null){
                zk.create(this.path, ("分布式队列"+appCode+"-"+queueName+"根目录").getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        } catch (KeeperException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public int size() {
        try {
            return zk.getChildren(path ,false).size();
        } catch (KeeperException e) {
            logger.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        return 0;
    }

    @Override
    public boolean isEmpty() {
        try {
            return zk.getChildren(path ,false).size() == 0;
        } catch (KeeperException e) {
        } catch (InterruptedException e) {
        }
        return true;
    }

    private E getData(String _path_) throws KeeperException, InterruptedException {
        return (E) toObject(zk.getData(path + "/" +_path_ ,false,zk.exists(path + "/" +_path_, false)));
    }

    private List<String> getList(){
        try {
            return zk.getChildren(path ,false);
        } catch (KeeperException e) {
        } catch (InterruptedException e) {
        }
        return new ArrayList<>(0);
    }

    @Override
    public boolean contains(Object o) {
        Objects.requireNonNull(o);
        try {
            List<String> list = zk.getChildren(path ,false);
            for(String ipath: list){
                try {
                    byte[] data = zk.getData(path + "/" + ipath, false, zk.exists(path + "/" + ipath, false));
                    if(o.equals(toObject(data))){
                        return true;
                    }
                } catch (KeeperException e) {
                } catch (InterruptedException e) {
                }
            }
        } catch (KeeperException e) {
        } catch (InterruptedException e) {
        }
        return false;
    }

    @Override
    public Iterator<E> iterator() {
        return new Itr();
    }

    private class Itr implements Iterator<E> {

        String current;
        E currentE;
        String next;
        E nextE;

        ZooKeeper itrZk;


        public Itr(){
            try {
                this.itrZk = new ZooKeeper(zookeeperConfig, sessionTimeout, new Watcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) {
                        if(logger.isDebugEnabled())
                            logger.debug("init itrZk successfully!");
                    }
                });
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }

        private E getData(String _path_){
            try {
               return (E) toObject(itrZk.getData(path + "/" +_path_ ,false,zk.exists(path + "/" +_path_, false)));
            } catch (KeeperException e) {
            } catch (InterruptedException e) {
            }
            return null;
        }

        private List<String> getList(){
            try {
                return itrZk.getChildren(path ,false);
            } catch (KeeperException e) {
            } catch (InterruptedException e) {
            }
            return null;
        }


        NumberFormat df = new java.text.DecimalFormat("0000000000");

        public boolean hasNext() {
            E e = null;
            String _path_ = null;
            List<String> list = getList();
            if(CollectionUtils.isEmpty(list))
                return false;
            Collections.sort(list);
            int max = Integer.parseInt(list.get(list.size()-1));
            if(current != null) {
                _path_ = current;
                while(Integer.parseInt(_path_ = String.valueOf(df.format((Integer.valueOf(_path_) + 1)))) <= max) {
                    if ((e = getData(_path_)) != null) {
                        next = _path_;
                        nextE = e;
                        break;
                    }
                }
                return e != null;
            }else{
                _path_ = list.get(0);
                while(Integer.parseInt(_path_ = String.valueOf(df.format((Integer.valueOf(_path_) + 1)))) <= max) {
                    if ((e = getData(_path_)) != null) {
                        next = _path_;
                        nextE = e;
                        break;
                    }
                }
                return e != null;
            }
        }

        public E next() {
            currentE = nextE;
            current = next;
            return nextE;
        }

        public void remove() {
            List<String> list = getList();
            if(!CollectionUtils.isEmpty(list)){
                try {
                    Collections.sort(list);
                    itrZk.delete(path + "/" + list.get(list.size()-1), -1);
                } catch (InterruptedException e) {
                } catch (KeeperException e) {
                    remove();
                }
            }
        }

        @Override
        public void forEachRemaining(Consumer<? super E> action) {
//            Objects.requireNonNull(action);
            throw new UnsupportedOperationException("distributed queue not support stream of jdk1.8");
        }

    }

    @Override
    public Object[] toArray() {
        try {
            List<String> list = zk.getChildren(path ,false);
            return list.stream().map(ipath -> {
                try {
                    return (Object) zk.getData(path + "/" + ipath, false, zk.exists(path + "/" + ipath, false));
                } catch (KeeperException e) {
                } catch (InterruptedException e) {
                }
                return null;
            }).collect(Collectors.toList()).toArray();
        } catch (KeeperException e) {
        } catch (InterruptedException e) {
        }
        return null;
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return null;
    }

    @Override
    public boolean add(E e) {
        try {
            String myZnode = zk.create(path + "/", toByteArray((Object)e), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT_SEQUENTIAL);
            return !StringUtils.isEmpty(myZnode);
        } catch (KeeperException ex) {
        } catch (InterruptedException ex) {
        }
        return false;
    }

    @Override
    public boolean remove(Object o) {
        Objects.requireNonNull(o);
        try {
            List<String> list = zk.getChildren(path ,false);
            Collections.sort(list);
            for(String ipath: list){
                try {
                    if(o.equals(getData(ipath))){
                        zk.delete(ipath, -1);
                        return true;
                    }
                } catch (KeeperException e) {
                } catch (InterruptedException e) {
                }
            }
            return false;
        } catch (KeeperException e) {
            logger.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        boolean b = true;
        for(Object obj: c){
            b &= contains(obj);
        }
        return b;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean b = true;
        for(E e: c){
            b &= add(e);
        }
        return b;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean b = true;
        for(Object o: c){
            b &= remove(o);
        }
        return b;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {
        List<String> list = getList();
        Collections.sort(list);
        for(String s:list){
            try {
                zk.delete(path + "/" + s, -1);
            } catch (InterruptedException e) {
            } catch (KeeperException e) {
            }
        }
    }

    @Override
    public boolean offer(E e) {
        return false;
    }

    @Override
    public E remove() {
        List<String> list = getList();
        E e;
        if(CollectionUtils.isEmpty(list))
            throw new IllegalStateException();
        try {
            if((e = getData(list.get(0))) == null)
                return remove();
            zk.delete(path + "/" + list.get(0), -1);
            return e;
        } catch (KeeperException | InterruptedException ex) {
            return remove();
        }
    }

    @Override
    public E poll() {
        List<String> list = getList();
        E e;
        if(CollectionUtils.isEmpty(list))
            return null;
        try {
            Collections.sort(list);
            if((e = getData(list.get(0))) == null)
                return poll();
            zk.delete(path + "/" + list.get(0), -1);
            return e;
        } catch (KeeperException | InterruptedException ex) {
            return poll();
        }
    }

    @Override
    public E element() {
        List<String> list = getList();
        E e;
        if(CollectionUtils.isEmpty(list))
            throw new IllegalStateException();
        try {
            Collections.sort(list);
            if((e = getData(list.get(0))) == null)
                return element();
            return e;
        } catch (KeeperException | InterruptedException ex) {
            return remove();
        }
    }

    @Override
    public E peek() {
        List<String> list = getList();
        E e;
        if(CollectionUtils.isEmpty(list))
            return null;
        try {
            Collections.sort(list);
            if((e = getData(list.get(0))) == null)
                return peek();
            return e;
        } catch (KeeperException | InterruptedException ex) {
            return peek();
        }
    }

    public byte[] toByteArray (Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray ();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        }
        return bytes;
    }

    /**
     * 数组转对象
     * @param bytes
     * @return
     */
    public Object toObject (byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream (bytes);
            ObjectInputStream ois = new ObjectInputStream (bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        } catch (ClassNotFoundException ex) {
            logger.error(ex.getMessage(), ex);
        }
        return obj;
    }
}
