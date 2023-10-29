/*
 * Copyright 2023 Kapeta Inc.
 * SPDX-License-Identifier: MIT
 */

package com.kapeta.spring.mongo;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kapeta.spring.config.providers.KapetaConfigurationProvider;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.mongo.MongoClientSettingsBuilderCustomizer;
import org.springframework.boot.autoconfigure.mongo.MongoConnectionDetails;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.boot.autoconfigure.mongo.PropertiesMongoConnectionDetails;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;

import java.util.Arrays;
import java.util.Optional;

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
    public KapetaConfigurationProvider.ResourceInfo mongoInfo() {
        return configurationProvider.getResourceInfo(RESOURCE_TYPE, PORT_TYPE, resourceName);
    }
    @Bean
    public PropertiesMongoConnectionDetails mongoConnectionDetails(KapetaConfigurationProvider.ResourceInfo mongoInfo) {
        String databaseName = String.valueOf(mongoInfo.getOptions().getOrDefault("dbName", resourceName));
        String dbAuthDB = String.valueOf(mongoInfo.getOptions().getOrDefault("authdb", "admin"));
        MongoProperties properties = new MongoProperties();
        properties.setDatabase(databaseName);
        properties.setHost(mongoInfo.getHost());
        properties.setPort(Integer.valueOf(mongoInfo.getPort()));
        properties.setUsername(mongoInfo.getCredentials().get("username"));
        properties.setPassword(mongoInfo.getCredentials().getOrDefault("password","").toCharArray());
        properties.setAuthenticationDatabase(dbAuthDB);
        properties.setAutoIndexCreation(true);
        return new PropertiesMongoConnectionDetails(properties);
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
