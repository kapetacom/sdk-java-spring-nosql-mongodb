package com.blockware.spring.mongo.sharding;


import java.lang.annotation.*;

/**
 * Add this to entity classes already annotated with @Document to specific a shard key for the given
 * collection
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface ShardKey {
    String def() default "";
}
