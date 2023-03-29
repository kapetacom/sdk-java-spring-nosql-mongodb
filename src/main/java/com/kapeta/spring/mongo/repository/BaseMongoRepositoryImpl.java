package com.kapeta.spring.mongo.repository;

import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.support.SimpleMongoRepository;

/**
 * Extended base mongo repository implementation to expose MongoOperations and metadata as part of the standard interface.
 *
 * This makes it possible to do a lot with just interfaces since Java8 default methods have access to getMetadata and
 * getMongoOperations for custom logic.
 *
 * @param <T>
 */
public class BaseMongoRepositoryImpl<T, ID> extends SimpleMongoRepository<T, ID> implements BaseMongoRepository<T, ID> {

    private final MongoEntityInformation<T, ID> metadata;

    private final MongoOperations mongoOperations;

    /**
     * Creates a new {@link SimpleMongoRepository} for the given {@link MongoEntityInformation} and {@link MongoTemplate}.
     *
     * @param metadata        must not be {@literal null}.
     * @param mongoOperations must not be {@literal null}.
     */
    public BaseMongoRepositoryImpl(MongoEntityInformation<T, ID> metadata,
                                   MongoOperations mongoOperations) {
        super(metadata, mongoOperations);

        this.metadata = metadata;

        this.mongoOperations = mongoOperations;
    }

    @Override
    public MongoEntityInformation<T, ID> getMetadata() {
        return metadata;
    }

    @Override
    public MongoOperations getMongoOperations() {
        return mongoOperations;
    }
}
