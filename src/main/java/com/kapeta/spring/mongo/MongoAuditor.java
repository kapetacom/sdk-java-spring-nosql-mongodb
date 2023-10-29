/*
 * Copyright 2023 Kapeta Inc.
 * SPDX-License-Identifier: MIT
 */
package com.kapeta.spring.mongo;


import org.springframework.data.domain.AuditorAware;

import java.util.Optional;

/**
 * Implementation for the MongoDB layer that adds automatically setting CreatedBy and ModifiedBy values
 * to the current user
 */
public class MongoAuditor implements AuditorAware<String> {

    @Override
    public Optional<String> getCurrentAuditor() {
        return Optional.empty();
    }
}
