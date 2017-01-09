package com.dooioo.cluster;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Created by dingxin on 16/12/15.
 */
public class ClusterConfiguration {

    private static final Object lock = new Object();

    private static final Log logger = LogFactory.getLog(ClusterConfiguration.class);

    private static volatile ClusterConfiguration clusterConfiguration;

    private Properties clusterProperties;
    private String env;
    private String appCode;
    private String zookeeperConfig;

    private ClusterConfiguration(){
        InputStream in = ClusterConfiguration.class.getClassLoader().getResourceAsStream("cluster.config.properties");
        clusterProperties = new Properties();
        try {
            clusterProperties.load(in);
        } catch (IOException e) {
            logger.info(e.getMessage() ,e);
        }
    }

    public static ClusterConfiguration getInstance(){
        if(clusterConfiguration == null){
            synchronized (lock){
                if(clusterConfiguration == null)
                    clusterConfiguration = new ClusterConfiguration();
            }
        }
        return clusterConfiguration;
    }

    public String getProperty(String key){
        return (String) clusterProperties.get(key);
    }

    public String getEnv() {
        return env;
    }

    public void setEnv(String env) {
        this.env = env;
    }

    public String getAppCode() {
        return appCode;
    }

    public void setAppCode(String appCode) {
        this.appCode = appCode;
    }

    public String getZookeeperConfig() {
        if(zookeeperConfig == null) {
            synchronized (lock) {
                if(zookeeperConfig == null){
                    try {
                        parseZookeeperConfig();
                    } catch (Exception e) {
                        logger.info(e.getMessage() ,e);
                    }
                }
            }
        }
        return zookeeperConfig;
    }

    private void parseZookeeperConfig() throws Exception {
        zookeeperConfig = "";
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("cluster/"+env + ".cluster.properties");
        Properties properties = new Properties();
        properties.load(in);
        Set<Object> keys = properties.keySet();
        Pattern p = Pattern.compile("^.+[:]\\d{1,5}\\s*$");
        for(Object key :keys) {
            if(key == null)
                continue;
            String val = (String) key;
            if(!val.startsWith("zookeeperNode"))
                continue;
            if(!StringUtils.isEmpty(zookeeperConfig))
                zookeeperConfig += ",";
            val = (String) properties.get(val);
            if (!p.matcher(val).matches()) {
                throw new IllegalArgumentException("非法的zookeeper节点配置！");
            }
            zookeeperConfig += val;
        }
    }
}

