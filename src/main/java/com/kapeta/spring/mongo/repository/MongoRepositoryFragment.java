package com.kapeta.spring.mongo.repository;

import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.repository.NoRepositoryBean;

/**
 * Part of the BaseMongoRepository setup that allows Java8 default interface methods to access these methods
 * @param <T>
 */
@NoRepositoryBean
public interface MongoRepositoryFragment<T, ID> {
    MongoEntityInformation<T, ID> getMetadata();

    MongoOperations getMongoOperations();
}
