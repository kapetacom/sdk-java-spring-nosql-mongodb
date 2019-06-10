package com.blockware.spring.mongo.sharding;


import com.mongodb.BasicDBObject;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * Configuration that enables sharding when available on the configured MongoDB database
 */
public class MongoShardingConfig {
    private static final Logger log = LoggerFactory.getLogger(MongoShardingConfig.class);

    @Autowired
    private MongoTemplate mongoTemplate;


    @Bean
    public MongoPersistentEntityShardKeyCreator mongoPersistentEntityShardKeyCreator() {
        return new MongoPersistentEntityShardKeyCreator();
    }

    @Autowired
    public void configureSharding(@Qualifier("adminDb") MongoDatabase adminDb) {

        try {

            BasicDBObject enableShardingCmd = new BasicDBObject("enableSharding", mongoTemplate.getDb().getName());
            adminDb.runCommand(enableShardingCmd);
        } catch (MongoCommandException ex) {
            if (ex.getErrorCode() == -1) {
                log.info("Sharding already enabled for db: {}", mongoTemplate.getDb().getName());
                return;
            }

            if (ex.getErrorCode() == 59) {
                log.warn("Command not found - not connected to cluster (mongos)? [Error: {}] Continuing...", ex.getErrorMessage());
                return;
            }

            throw ex;
        }
    }
}
