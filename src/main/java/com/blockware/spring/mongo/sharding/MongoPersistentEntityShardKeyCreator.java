package com.blockware.spring.mongo.sharding;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.context.MappingContextEvent;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handler for the ShardKey.class annotation - automatically creating the index if needed and applies the shard key
 * to the collection as defined.
 */
public class MongoPersistentEntityShardKeyCreator implements ApplicationListener<MappingContextEvent<?, ?>> {

    private static final Logger log = LoggerFactory.getLogger(MongoPersistentEntityShardKeyCreator.class);

    private final Map<Class<?>, Boolean> classesSeen = new ConcurrentHashMap<>();

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    @Qualifier("adminDb")
    private MongoDatabase adminDb;

    @Override
    public void onApplicationEvent(MappingContextEvent<?, ?> event) {

        final MongoMappingContext mappingContext = getMappingContext();

        if (!event.wasEmittedBy(mappingContext)) {
            return;
        }

        PersistentEntity<?, ?> entity = event.getPersistentEntity();

        // Double check type as Spring infrastructure does not consider nested generics
        if (entity instanceof MongoPersistentEntity) {
            checkForShardKey((MongoPersistentEntity<?>) entity);
        }
    }

    private MongoMappingContext getMappingContext() {
        return (MongoMappingContext) mongoTemplate.getConverter().getMappingContext();
    }

    private void checkForShardKey(MongoPersistentEntity<?> entity) {
        Class<?> type = entity.getType();

        if (!classesSeen.containsKey(type)) {

            this.classesSeen.put(type, Boolean.TRUE);

            if (log.isDebugEnabled()) {
                log.debug("Analyzing class {} for shard key information.", type);
            }

            checkForAndCreateShardKey(entity);
        }
    }

    private void checkForAndCreateShardKey(MongoPersistentEntity<?> entity) {
        if (entity.isAnnotationPresent(Document.class)) {
            final MongoMappingContext mappingContext = getMappingContext();

            BasicMongoPersistentEntity<?> root = mappingContext.getRequiredPersistentEntity(entity.getTypeInformation());
            String collection = root.getCollection();

            ShardKey shardKey = root.findAnnotation(ShardKey.class);

            createShardKey(collection, shardKey);

        }
    }

    private void createShardKey(String collectionName, ShardKey shardKeyDefinition) {
        if (shardKeyDefinition == null) {
            log.warn("No shard key defined for collection: {}", collectionName);
            return;
        }
        org.bson.Document shardKey = org.bson.Document.parse(shardKeyDefinition.def());
        final BasicDBObject shardKeyObject = new BasicDBObject();
        for (String key : shardKey.keySet()) {
            shardKeyObject.put(key, shardKey.getInteger(key, 1));
        }

        IndexOptions indexOptions = new IndexOptions();
        indexOptions.background(false);
        mongoTemplate.getCollection(collectionName).createIndex(shardKeyObject, indexOptions);

        final BasicDBObject shardCollectionCmd = new BasicDBObject(
                "shardCollection",
                String.format("%s.%s", mongoTemplate.getDb().getName(), collectionName)
        );
        shardCollectionCmd.put("key", shardKeyObject);
        try {
            adminDb.runCommand(shardCollectionCmd);
        } catch (MongoCommandException ex) {
            if (ex.getErrorCode() == -1) {
                log.debug("Shard key already applied for collection: {}", collectionName);
                return;
            }

            if (ex.getErrorCode() == 59) {
                log.warn("Command not found - not connected to a cluster (mongos)? [Error: {}] Continuing...", ex.getErrorMessage());
                return;
            }

            throw ex;
        }
    }

}
