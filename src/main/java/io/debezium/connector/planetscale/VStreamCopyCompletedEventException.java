/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.planetscale;

/**
 * Used to signal that Debezium should not continue consuming the streaming
 * event source once snapshot is completed and snapshot.mode=initial_only.
 */
public class VStreamCopyCompletedEventException extends RuntimeException {
    public VStreamCopyCompletedEventException(String message) {
        super(message);
    }
}
