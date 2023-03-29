package com.kapeta.spring.annotation;

import org.springframework.data.mongodb.config.EnableMongoAuditing;

import java.lang.annotation.*;

/**
 * Add this to your Application class to enable mongodb support
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited

@EnableMongoAuditing(
        auditorAwareRef = "mongoAuditor",
        modifyOnCreate = true,
        setDates = true
)
public @interface KapetaEnableMongoDB {

}