package com.dooioo.cluster;

import com.dooioo.cluster.redis.JedisClusterFactory;
import com.dooioo.extension.lock.DistributedLockInterceptor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.annotation.*;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import redis.clients.jedis.JedisCluster;

import java.io.*;

/**
 * Created by dingxin on 16/12/6.
 */
@Configuration
@ImportResource("classpath*:spring/spring-ext.xml")
@ComponentScan({"com.dooioo.redis", "com.dooioo.extension"})
public class ClusterRegistrar {

    private static final Log logger = LogFactory.getLog(ClusterRegistrar.class);

    ClusterConfiguration configuration = ClusterConfiguration.getInstance();

    private String appCode;
    private String env;

    public ClusterRegistrar(){
        logger.info("register beans of cluster!");
    }


    @Bean(name = "distributedLockInterceptor")
    public DistributedLockInterceptor registerDistributedLockInterceptor(){
        DistributedLockInterceptor distributedLockInterceptor = new DistributedLockInterceptor();
        return distributedLockInterceptor;
    }

    @Bean(name = "jedisCluster")
    public FactoryBean<JedisCluster> registerJedisClusterFactory() throws FileNotFoundException {
        Resource resource = new ClassPathResource("cluster/"+env+".cluster.properties",ClusterRegistrar.class.getClassLoader());
        FactoryBean<JedisCluster> factory = new JedisClusterFactory(resource,
                configuration.getProperty("redis.cluster.addressKeyPrefix"), Integer.parseInt(configuration.getProperty("redis.cluster.timeout")), Integer.parseInt(configuration.getProperty("redis.cluster.maxRedirections")), this.registerGenericObjectPoolConfig());
        return factory;
    }

    @Bean(name = "genericObjectPoolConfig")
    public GenericObjectPoolConfig registerGenericObjectPoolConfig(){
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxWaitMillis(Long.parseLong(configuration.getProperty("redis.cluster.maxWaitMillis")));
        genericObjectPoolConfig.setMaxTotal(Integer.parseInt(configuration.getProperty("redis.cluster.maxTotal")));
        genericObjectPoolConfig.setMinIdle(Integer.parseInt(configuration.getProperty("redis.cluster.minIdel")));
        genericObjectPoolConfig.setMaxIdle(Integer.parseInt(configuration.getProperty("redis.cluster.maxIdel")));
        return genericObjectPoolConfig;
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
        configuration.setEnv(env);
    }

    public String getAppCode() {
        return appCode;
    }

    public void setAppCode(String appCode) {
        this.appCode = appCode;
        configuration.setAppCode(appCode);
    }

}
