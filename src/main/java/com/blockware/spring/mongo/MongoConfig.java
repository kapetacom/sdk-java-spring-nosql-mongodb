package com.blockware.spring.mongo;


import com.blockware.spring.cluster.BlockwareClusterService;
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
public class MongoConfig {

    private static final String RESOURCE_TYPE = "nosqldb.blockware.com/v1/mongodb";

    private static final String PORT_TYPE = "mongodb";

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${spring.application.name}")
    private String applicationName;

    private String dbAuthDB;

    @Autowired
    private BlockwareClusterService blockwareConfigSource;

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

    private String getDatabaseName() {
        return applicationName;
    }

    @Bean
    public MongoDbFactory mongoDbFactory(MongoClient mongoClient) {
        final SimpleMongoDbFactory simpleMongoDbFactory = new SimpleMongoDbFactory(mongoClient, getDatabaseName());

        log.info("Using mongodb database: {}", getDatabaseName());

        return simpleMongoDbFactory;
    }

    @Bean
    public MongoTemplate mongoTemplate(MongoDbFactory factory, MongoConverter mongoConverter) {
        return new MongoTemplate(factory, mongoConverter);
    }

    @Bean("adminDb")
    public MongoDatabase adminDb(MongoTemplate template) {
        return template.getMongoDbFactory().getDb(dbAuthDB);
    }

    @Bean
    public MongoClient createClient() {
        final BlockwareClusterService.ResourceInfo mongoInfo = blockwareConfigSource.getResourceInfo(RESOURCE_TYPE, PORT_TYPE);
        Optional<String> dbUsername = Optional.ofNullable(mongoInfo.getCredentials().get("username"));
        Optional<String> dbPassword = Optional.ofNullable(mongoInfo.getCredentials().get("password"));
        Optional<Boolean> dbAuthCR = Optional.ofNullable((Boolean)mongoInfo.getOptions().get("auth_cr"));
        dbAuthDB = String.valueOf(mongoInfo.getOptions().getOrDefault("authdb", "admin"));

        ServerAddress serverAddress = new ServerAddress(mongoInfo.getHost(), Integer.valueOf(mongoInfo.getPort()));


        log.info("Connecting to mongodb server: {}:{}", mongoInfo.getHost(), mongoInfo.getPort());

        MongoClient client;
        MongoClientOptions options = MongoClientOptions.builder()
                .writeConcern(WriteConcern.JOURNALED)
                .build();


        if (dbUsername.isPresent() &&
                dbUsername.get() != null &&
                !dbUsername.get().trim().isEmpty()) {

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


    @Bean("mongoAuditor")
    public MongoAuditor mongoAuditor() {
        return new MongoAuditor();
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
