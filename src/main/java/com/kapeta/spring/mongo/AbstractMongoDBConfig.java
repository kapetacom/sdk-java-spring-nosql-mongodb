/*
 * Copyright 2023 Kapeta Inc.
 * SPDX-License-Identifier: MIT
 */

package com.kapeta.spring.mongo;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kapeta.spring.config.providers.KapetaConfigurationProvider;
import com.kapeta.spring.config.providers.types.ResourceInfo;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.mongo.MongoClientSettingsBuilderCustomizer;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.boot.autoconfigure.mongo.PropertiesMongoConnectionDetails;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.lang.NonNull;

import java.util.Arrays;

/**
 * Mongo configuration class.
 */
@Slf4j
abstract public class AbstractMongoDBConfig {

    private static final String RESOURCE_TYPE = "kapeta/resource-type-mongodb";

    private static final String PORT_TYPE = "mongodb";

    @Autowired
    private KapetaConfigurationProvider configurationProvider;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${spring.application.name}")
    private String applicationName;

    private final String resourceName;

    protected AbstractMongoDBConfig(String resourceName) {
        this.resourceName = resourceName;
    }

    @Bean("mongoInfo")
    public ResourceInfo mongoInfo() {
        return configurationProvider.getResourceInfo(RESOURCE_TYPE, PORT_TYPE, resourceName);
    }

    @Bean
    public PropertiesMongoConnectionDetails mongoConnectionDetails(ResourceInfo mongoInfo) {
        String databaseName = String.valueOf(mongoInfo.getOptions().getOrDefault("dbName", resourceName));
        String dbAuthDB = String.valueOf(mongoInfo.getOptions().getOrDefault("authdb", "admin"));
        String protocol = String.valueOf(mongoInfo.getOptions().getOrDefault("protocol", "mongodb"));

        MongoProperties properties;
        if ("mongodb+srv".equals(protocol)) {
            properties = createMongoUriProperties(databaseName, dbAuthDB, mongoInfo);
        } else {
            properties = createMongoProperties(databaseName, dbAuthDB, mongoInfo);
        }
        return new PropertiesMongoConnectionDetails(properties);
    }

    @NonNull
    private MongoProperties createMongoProperties(String databaseName, String dbAuthDB, ResourceInfo mongoInfo) {
        MongoProperties properties = new MongoProperties();
        properties.setDatabase(databaseName);
        properties.setHost(mongoInfo.getHost());
        properties.setPort(Integer.valueOf(mongoInfo.getPort()));
        properties.setUsername(mongoInfo.getCredentials().get("username"));
        properties.setPassword(mongoInfo.getCredentials().getOrDefault("password","").toCharArray());
        properties.setAuthenticationDatabase(dbAuthDB);
        properties.setAutoIndexCreation(true);
        return properties;
    }

    private MongoProperties createMongoUriProperties(String databaseName, String dbAuthDB, ResourceInfo mongoInfo) {
        String username = mongoInfo.getCredentials().get("username");
        String password = mongoInfo.getCredentials().getOrDefault("password","");

        String uri = String.format("mongodb+srv://%s:%s@%s/%s?ssl=false&authSource=%s", username, password, mongoInfo.getHost(), databaseName, dbAuthDB);

        MongoProperties properties = new MongoProperties();
        properties.setUri(uri);
        return properties;
    }

    @Bean
    public MongoClientSettingsBuilderCustomizer customizer() {
        return settings -> settings.applicationName(applicationName);
    }

    @Bean
    public MongoTransactionManager transactionManager(MongoDatabaseFactory dbFactory) {
        return new MongoTransactionManager(dbFactory);
    }

    @Bean
    public MongoCustomConversions objectNodeConverters() {

        return new MongoCustomConversions(Arrays.asList(
                new MongoToJackson(),
                new JacksonToMongo()
        ));
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
