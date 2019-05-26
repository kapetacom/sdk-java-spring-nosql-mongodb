package com.blockware.spring.annotation;

import com.blockware.spring.mongo.MongoConfig;
import com.blockware.spring.mongo.repository.BaseMongoRepositoryImpl;
import com.blockware.spring.mongo.sharding.MongoShardingConfig;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.lang.annotation.*;

/**
 * Add this to your Application class to enable mongodb support
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
@EnableMongoRepositories(
        repositoryBaseClass = BaseMongoRepositoryImpl.class
)
@EnableMongoAuditing(
        auditorAwareRef = "mongoAuditor",
        modifyOnCreate = true,
        setDates = true
)
@Import({
        MongoConfig.class,
        MongoShardingConfig.class
})
public @interface BlockwareEnableMongoDB {
}
