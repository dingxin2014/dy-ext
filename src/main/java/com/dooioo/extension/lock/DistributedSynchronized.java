package com.dooioo.extension.lock;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * 分布式同步
 *  <p>value is lock name, same name means that they share the lock</p>
 * @author dingxin
 *
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DistributedSynchronized {
	
	String value() default "";

}
