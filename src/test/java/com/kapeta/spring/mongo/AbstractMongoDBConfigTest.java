/*
 * Copyright 2023 Kapeta Inc.
 * SPDX-License-Identifier: MIT
 */
package com.kapeta.spring.mongo;

import com.kapeta.spring.config.providers.TestConfigProvider;
import com.kapeta.spring.config.providers.types.ResourceInfo;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;


import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AbstractMongoDBConfigTest {


    @Test
    public void testCreateMongoUriProperties() {
        Map<String, String> credentials = new HashMap<>();
        credentials.put("username", "testUser");
        credentials.put("password", "testPass");

        Map<String, Object> options = new HashMap<>();
        options.put("ssl", "true");

        TestMongoDBConfig mongoDBConfig = new TestMongoDBConfig("testResource");

        ResourceInfo resourceInfo = new ResourceInfo();
        resourceInfo.setCredentials(credentials);
        resourceInfo.setOptions(options);
        resourceInfo.setHost("testHost");

        MongoProperties properties = mongoDBConfig.createMongoUriProperties("testDB", "admin", resourceInfo);

        assertEquals("mongodb+srv://testUser:testPass@testHost/testDB?ssl=true&authSource=admin", properties.getUri());
    }


    @Test
    public void testSSLFalse() {
        Map<String, String> credentials = new HashMap<>();
        credentials.put("username", "testUser");
        credentials.put("password", "testPass");

        Map<String, Object> options = new HashMap<>();
        options.put("ssl", "false");

        TestMongoDBConfig mongoDBConfig = new TestMongoDBConfig("testResource");

        ResourceInfo resourceInfo = new ResourceInfo();
        resourceInfo.setCredentials(credentials);
        resourceInfo.setOptions(options);
        resourceInfo.setHost("testHost");

        MongoProperties properties = mongoDBConfig.createMongoUriProperties("testDB", "admin", resourceInfo);

        assertEquals("mongodb+srv://testUser:testPass@testHost/testDB?ssl=false&authSource=admin", properties.getUri());
    }

    @Test
    public void testEmptySSLConfig() {
        Map<String, String> credentials = new HashMap<>();
        credentials.put("username", "testUser");
        credentials.put("password", "testPass");

        Map<String, Object> options = new HashMap<>();

        TestMongoDBConfig mongoDBConfig = new TestMongoDBConfig("testResource");

        ResourceInfo resourceInfo = new ResourceInfo();
        resourceInfo.setCredentials(credentials);
        resourceInfo.setOptions(options);
        resourceInfo.setHost("testHost");

        MongoProperties properties = mongoDBConfig.createMongoUriProperties("testDB", "admin", resourceInfo);

        assertEquals("mongodb+srv://testUser:testPass@testHost/testDB?ssl=false&authSource=admin", properties.getUri());
    }

    @Test
    public void testEmptyAuthSource() {
        Map<String, String> credentials = new HashMap<>();
        credentials.put("username", "testUser");
        credentials.put("password", "testPass");

        Map<String, Object> options = new HashMap<>();
        options.put("ssl", "true");

        TestMongoDBConfig mongoDBConfig = new TestMongoDBConfig("testResource");

        ResourceInfo resourceInfo = new ResourceInfo();
        resourceInfo.setCredentials(credentials);
        resourceInfo.setOptions(options);
        resourceInfo.setHost("testHost");

        MongoProperties properties = mongoDBConfig.createMongoUriProperties("testDB", "", resourceInfo);

        assertEquals("mongodb+srv://testUser:testPass@testHost/testDB?ssl=true", properties.getUri());
    }

    private class TestMongoDBConfig extends AbstractMongoDBConfig {
        public TestMongoDBConfig(String resourceName) {
            super(resourceName);
        }
    }
}