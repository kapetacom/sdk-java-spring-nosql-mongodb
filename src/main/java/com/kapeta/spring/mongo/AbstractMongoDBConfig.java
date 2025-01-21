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
import org.apache.commons.lang3.StringUtils;
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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Mongo configuration class.
 */
@Slf4j
abstract public class AbstractMongoDBConfig {

    private static final String RESOURCE_TYPE = "kapeta/resource-type-mongodb";

    private static final String PORT_TYPE = "mongodb";

    private static final String[] SUPPORTED_MONGO_OPTIONS = { "maxIdleTimeMS", "ssl"};

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

    protected MongoProperties createMongoUriProperties(String databaseName, String dbAuthDB, ResourceInfo mongoInfo) {
        String username = mongoInfo.getCredentials().get("username");
        String password = mongoInfo.getCredentials().getOrDefault("password","");

        Map<String, String> options = new HashMap<>();
        for (String option : SUPPORTED_MONGO_OPTIONS) {
            if (mongoInfo.getOptions().containsKey(option)) {
                options.put(option, mongoInfo.getOptions().get(option).toString());
            }
        }

        if (!StringUtils.isBlank(dbAuthDB)) {
            options.put("authSource", dbAuthDB);
        }

        String formattedOptions = options.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining("&"));

        if (!StringUtils.isBlank(formattedOptions)) {
            formattedOptions = "?" + formattedOptions;
        }

        String uri = String.format("mongodb+srv://%s:%s@%s/%s%s", username, password, mongoInfo.getHost(), databaseName, formattedOptions);

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
