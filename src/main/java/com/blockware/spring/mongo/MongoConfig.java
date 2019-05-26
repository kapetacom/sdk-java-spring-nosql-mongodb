package com.blockware.spring.mongo;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.*;
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
public class MongoConfig {

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${mongo.host:localhost}")
    private String dbHost;

    @Value("${mongo.port:27017}")
    private int dbPort;

    @Value("${mongo.user:#{null}}")
    private Optional<String> dbUsername;

    @Value("${mongo.pass:#{null}}")
    private Optional<String> dbPassword;

    @Value("${mongo.auth_cr:false}")
    private boolean dbAuthCR;

    @Value("${mongo.db:test}")
    private String dbName;

    @Value("${mongo.auth_db:admin}")
    private String dbAuthDB;

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
    public MongoDbFactory mongoDbFactory(MongoClient mongoClient) {
        return new SimpleMongoDbFactory(mongoClient, dbName);
    }

    @Bean
    public MongoTemplate mongoTemplate(MongoDbFactory factory, MongoConverter mongoConverter) {
        return new MongoTemplate(factory, mongoConverter);
    }

    @Bean
    public MongoClient createClient() {
        ServerAddress serverAddress = new ServerAddress(dbHost, dbPort);

        MongoClient client;
        MongoClientOptions options = MongoClientOptions.builder()
                .writeConcern(WriteConcern.JOURNALED)
                .build();

        if (dbUsername.isPresent() &&
                dbUsername.get() != null &&
                !dbUsername.get().trim().isEmpty()) {

            MongoCredential dbCredentials = null;
            if (dbAuthCR) {
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
