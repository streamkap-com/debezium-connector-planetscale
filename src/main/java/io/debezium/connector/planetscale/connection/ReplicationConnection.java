/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.planetscale.connection;

import java.util.concurrent.atomic.AtomicReference;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.planetscale.Vgtid;
import io.debezium.connector.planetscale.VitessOffsetContext;

/**
 * A Vitess logical streaming replication connection. Replication connections are established from a
 * vtgate, starting from a specific {@link Vgtid}.
 */
@NotThreadSafe
public interface ReplicationConnection extends AutoCloseable {

    /**
     * Opens a stream that reads from a specific replication position.
     *
     * @param offsetContext a specific replication position
     * @param processor - a callback to which the decoded message is passed
     * @param error - check whether an error has happened during streaming, propagate the error
     *     asynchronously
     */
    void startStreaming(
                        VitessOffsetContext offsetContext, ReplicationMessageProcessor processor, AtomicReference<Throwable> error);
}
