package com.kapeta.spring.mongo;


import com.blockware.spring.cluster.BlockwareClusterService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.*;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

/**
 * Mongo configuration class.
 */
@Slf4j
abstract public class AbstractMongoDBConfig {

    private static final String RESOURCE_TYPE = "blockware/resource-type-mongodb";

    private static final String PORT_TYPE = "mongodb";

    @Autowired
    private BlockwareClusterService blockwareConfigSource;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${spring.application.name}")
    private String applicationName;

    private final String resourceName;

    private String databaseName;

    private String dbAuthDB;

    protected AbstractMongoDBConfig(String resourceName) {
        this.resourceName = resourceName;
    }

    @Bean
    public MongoClient createClient() {
        final BlockwareClusterService.ResourceInfo mongoInfo = blockwareConfigSource.getResourceInfo(RESOURCE_TYPE, PORT_TYPE, resourceName);
        Optional<String> dbUsername = Optional.ofNullable(mongoInfo.getCredentials().get("username"));
        Optional<String> dbPassword = Optional.ofNullable(mongoInfo.getCredentials().get("password"));

        dbAuthDB = String.valueOf(mongoInfo.getOptions().getOrDefault("authdb", "admin"));
        databaseName = String.valueOf(mongoInfo.getOptions().getOrDefault("dbName", resourceName));

        ServerAddress serverAddress = new ServerAddress(mongoInfo.getHost(), Integer.parseInt(mongoInfo.getPort()));

        log.info("Connecting to mongodb server: {}:{} for db: {}", mongoInfo.getHost(), mongoInfo.getPort(), databaseName);

        MongoClientSettings.Builder options = MongoClientSettings.builder()
                .writeConcern(WriteConcern.JOURNALED)
                .applicationName(applicationName)
                .applyToClusterSettings(cluster -> {
                    cluster.hosts(Collections.singletonList(serverAddress));
                });

        if (dbUsername.isPresent() &&
                !dbUsername.get().trim().isEmpty()) {
            options.credential(
                MongoCredential.createCredential(
                        dbUsername.get(),
                        dbAuthDB,
                        dbPassword.orElse("").toCharArray()
                )
            );
        }

        return MongoClients.create(options.build());
    }

    @Bean
    public MongoDatabaseFactory mongoDbFactory(MongoClient mongoClient) {
        final SimpleMongoClientDatabaseFactory simpleMongoDbFactory = new SimpleMongoClientDatabaseFactory(mongoClient, databaseName);

        log.info("Using mongodb database: {}", databaseName);

        return simpleMongoDbFactory;
    }

    @Bean
    public MongoConverter mongoConverter(MongoDatabaseFactory factory) {

        DbRefResolver dbRefResolver = new DefaultDbRefResolver(factory);

        MongoCustomConversions conversions = new MongoCustomConversions(Arrays.asList(
                new MongoToJackson(),
                new JacksonToMongo()
        ));

        MongoMappingContext mappingContext = new MongoMappingContext();
        mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
        mappingContext.afterPropertiesSet();

        MappingMongoConverter converter = new MappingMongoConverter(dbRefResolver, mappingContext);

        converter.setCustomConversions(conversions);
        converter.afterPropertiesSet();

        return converter;
    }

    @Bean
    public MongoTemplate mongoTemplate(MongoDatabaseFactory factory, MongoConverter mongoConverter) {
        return new MongoTemplate(factory, mongoConverter);
    }

    @Bean("adminDb")
    public MongoDatabase adminDb(MongoTemplate template) {
        final MongoDatabase adminDb = template.getMongoDatabaseFactory().getMongoDatabase(dbAuthDB);

        enableSharding(adminDb, template);

        return adminDb;
    }

    @Bean("mongoAuditor")
    public MongoAuditor mongoAuditor() {
        return new MongoAuditor();
    }


    private void enableSharding(MongoDatabase adminDb, MongoTemplate mongoTemplate) {

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

    @ReadingConverter
    private class MongoToJackson implements Converter<Document, ObjectNode> {

        @Override
        public ObjectNode convert(Document source) {

            if (source == null) {
                return null;
            }

            return objectMapper.convertValue(source, ObjectNode.class);
        }
    }

    @WritingConverter
    private class JacksonToMongo implements Converter<ObjectNode, Document> {

        @Override
        public Document convert(ObjectNode source) {

            if (source == null) {
                return null;
            }

            return objectMapper.convertValue(source, Document.class);
        }
    }

}
