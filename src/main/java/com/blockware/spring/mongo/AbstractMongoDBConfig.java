package com.blockware.spring.mongo;


import com.blockware.spring.cluster.BlockwareClusterService;
import com.blockware.spring.mongo.sharding.MongoPersistentEntityShardKeyCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.*;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import java.util.Arrays;
import java.util.Optional;

/**
 * Mongo configuration class.
 */
@Slf4j
abstract public class AbstractMongoDBConfig {

    private static final String RESOURCE_TYPE = "nosqldb.blockware.com/v1/mongodb";

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
        Optional<Boolean> dbAuthCR = Optional.ofNullable((Boolean)mongoInfo.getOptions().get("auth_cr"));
        dbAuthDB = String.valueOf(mongoInfo.getOptions().getOrDefault("authdb", "admin"));
        databaseName = String.valueOf(mongoInfo.getOptions().getOrDefault("dbName", resourceName));

        ServerAddress serverAddress = new ServerAddress(mongoInfo.getHost(), Integer.valueOf(mongoInfo.getPort()));

        log.info("Connecting to mongodb server: {}:{} for db: {}", mongoInfo.getHost(), mongoInfo.getPort(), databaseName);

        MongoClient client;
        MongoClientOptions options = MongoClientOptions.builder()
                .writeConcern(WriteConcern.JOURNALED)
                .applicationName(applicationName)
                .build();


        if (dbUsername.isPresent() && !dbUsername.get().trim().isEmpty()) {

            MongoCredential dbCredentials = null;
            if (dbAuthCR.isPresent() && dbAuthCR.get()) {
                dbCredentials = MongoCredential.createMongoCRCredential(
                        dbUsername.get(),
                        dbAuthDB,
                        dbPassword.orElse("").toCharArray());
            } else {
                dbCredentials = MongoCredential.createCredential(
                        dbUsername.get(),
                        dbAuthDB,
                        dbPassword.orElse("").toCharArray());
            }

            client = new MongoClient(serverAddress, dbCredentials, options);
        } else {
            client = new MongoClient(serverAddress, options);
        }

        return client;
    }

    @Bean
    public MongoDbFactory mongoDbFactory(MongoClient mongoClient) {
        final SimpleMongoDbFactory simpleMongoDbFactory = new SimpleMongoDbFactory(mongoClient, databaseName);

        log.info("Using mongodb database: {}", databaseName);

        return simpleMongoDbFactory;
    }

    @Bean
    public MongoConverter mongoConverter(MongoDbFactory factory) {

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
    public MongoTemplate mongoTemplate(MongoDbFactory factory, MongoConverter mongoConverter) {
        return new MongoTemplate(factory, mongoConverter);
    }

    @Bean("adminDb")
    public MongoDatabase adminDb(MongoTemplate template) {
        final MongoDatabase adminDb = template.getMongoDbFactory().getDb(dbAuthDB);

        enableSharding(adminDb, template);

        return adminDb;
    }

    @Bean("mongoAuditor")
    public MongoAuditor mongoAuditor() {
        return new MongoAuditor();
    }

    @Bean
    public MongoPersistentEntityShardKeyCreator mongoPersistentEntityShardKeyCreator() {
        return new MongoPersistentEntityShardKeyCreator();
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
