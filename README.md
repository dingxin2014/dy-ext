pom.xml

        <dependency>
           <groupId>dycore</groupId>
           <artifactId>dy-ext</artifactId>
           <version>1.0</version>
        </dependency>
        
        
spring.xml 中配置 

        <bean class="com.dooioo.cluster.ClusterRegistrar">
    		<property name="appCode" value="purchase"/>
    		<property name="env" value="test"/>
	    </bean>