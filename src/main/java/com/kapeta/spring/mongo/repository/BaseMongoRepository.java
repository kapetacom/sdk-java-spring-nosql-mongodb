/*
 * Copyright 2023 Kapeta Inc.
 * SPDX-License-Identifier: MIT
 */

package com.kapeta.spring.mongo.repository;


import com.mongodb.client.result.UpdateResult;
import org.springframework.core.annotation.Order;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;

import static org.springframework.data.mongodb.core.query.Criteria.where;

@NoRepositoryBean
@Order(20)
public interface BaseMongoRepository<T, ID> extends MongoRepositoryFragment<T, ID>, MongoRepository<T, ID> {

    /**
     * Helper method for getting the value of a single property instead of the entire document
     *
     * @param id
     * @param propertyName
     * @param propertyType
     * @param <U>
     * @return
     */
    default <U> U getProperty(ID id, String propertyName, Class<U> propertyType) {
        final MongoOperations mongo = getMongoOperations();

        Query query = new Query()
                .addCriteria(where("_id").is(id));

        query.fields().include(propertyName);

        T entity = mongo.findOne(query, getMetadata().getJavaType());

        if (entity == null) {
            return null;
        }

        try {

            final Field field = getMetadata().getJavaType().getDeclaredField(propertyName);
            ReflectionUtils.makeAccessible(field);
            return (U) field.get(entity);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("Invalid property name specified: " + propertyName, e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Could not access property: " + propertyName, e);
        }
    }

    /**
     * Helper method for updating a single property instead of updating the entire document.
     *
     * @param id
     * @param propertyName
     * @param propertyValue
     * @param <U>
     * @return
     */
    default <U> UpdateResult setProperty(ID id, String propertyName, U propertyValue) {
        final MongoOperations mongo = getMongoOperations();

        Query query = new Query()
                .addCriteria(where("_id").is(id));

        Update update = new Update()
                .set(propertyName, propertyValue);

        return mongo.updateFirst(query, update, getMetadata().getCollectionName());
    }

    /**
     * Helper method that transitions a property value to a new value only if the existing value matches
     *
     * Returns true if the document was updated - false otherwise
     * @param id
     * @param propertyName
     * @param from
     * @param to
     * @param <U>
     * @return
     */
    default <U> boolean setPropertyIf(ID id, String propertyName, U from, U to) {
        final MongoOperations mongo = getMongoOperations();

        Query query = new Query()
                .addCriteria(where("_id").is(id))
                .addCriteria(where(propertyName).is(from));

        Update update = new Update()
                .set(propertyName, to);

        final UpdateResult updateResult = mongo.updateFirst(query, update, getMetadata().getCollectionName());

        return updateResult.getModifiedCount() > 0;
    }

    /**
     * Helper method for adding/pushing a single value to a list property instead of updating the entire document.
     *
     * @param id
     * @param propertyName
     * @param propertyValue
     * @param <U>
     * @return
     */
    default <U> UpdateResult addValueToListProperty(ID id, String propertyName, U propertyValue) {
        final MongoOperations mongo = getMongoOperations();

        Query query = new Query()
                .addCriteria(where("_id").is(id));

        Update update = new Update()
                .push(propertyName, propertyValue);

        return mongo.updateFirst(query, update, getMetadata().getCollectionName());
    }

    /**
     * Helper method for adding/pushing a multiple values to a list property instead of updating the entire document.
     *
     * @param id
     * @param propertyName
     * @param propertyValue
     * @param <U>
     * @return
     */
    default <U> UpdateResult addToSetProperty(ID id, String propertyName, U propertyValue) {
        final MongoOperations mongo = getMongoOperations();

        Query query = new Query()
                .addCriteria(where("_id").is(id));

        Update update = new Update()
                .addToSet(propertyName, propertyValue);

        return mongo.updateFirst(query, update, getMetadata().getCollectionName());
    }

}
