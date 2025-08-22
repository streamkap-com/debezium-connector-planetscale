/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.planetscale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.planetscale.connection.ReplicationMessage;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.util.Clock;

class VitessSnapshotRecordEmitter extends SnapshotChangeRecordEmitter<VitessPartition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessSnapshotRecordEmitter.class);

    VitessSnapshotRecordEmitter(
                                VitessPartition partition,
                                VitessOffsetContext offsetContext,
                                Clock clock,
                                VitessConnectorConfig connectorConfig,
                                VitessDatabaseSchema schema,
                                ReplicationMessage message) {
        super(
                partition,
                offsetContext,
                VitessChangeRecordUtil.columnValues(
                        connectorConfig, schema, message.getNewTupleList(), VitessDatabaseSchema.parse(message.getTable())),
                clock,
                connectorConfig);
    }
}
