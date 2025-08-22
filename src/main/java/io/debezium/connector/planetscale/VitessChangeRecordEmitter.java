/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.planetscale;

import java.util.Objects;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.planetscale.connection.ReplicationMessage;
import io.debezium.data.Envelope;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

/**
 * Used by {@link EventDispatcher} to get the {@link SourceRecord} {@link Struct} and pass it to a
 * {@link Receiver}, which in turn enqueue the {@link SourceRecord} to {@link ChangeEventQueue}.
 */
class VitessChangeRecordEmitter extends RelationalChangeRecordEmitter<VitessPartition> {
    private final ReplicationMessage message;
    private final VitessDatabaseSchema schema;
    private final VitessConnectorConfig connectorConfig;
    private final TableId tableId;

    VitessChangeRecordEmitter(
                              VitessPartition partition,
                              VitessOffsetContext offsetContext,
                              Clock clock,
                              VitessConnectorConfig connectorConfig,
                              VitessDatabaseSchema schema,
                              ReplicationMessage message) {
        super(partition, offsetContext, clock, connectorConfig);

        this.schema = schema;
        this.message = message;
        this.connectorConfig = connectorConfig;
        this.tableId = VitessDatabaseSchema.parse(message.getTable());
        Objects.requireNonNull(tableId);
    }

    @Override
    public Envelope.Operation getOperation() {
        switch (message.getOperation()) {
            case INSERT:
                return Envelope.Operation.CREATE;
            case UPDATE:
                return Envelope.Operation.UPDATE;
            case DELETE:
                return Envelope.Operation.DELETE;
            case TRUNCATE:
                return Envelope.Operation.TRUNCATE;
            default:
                throw new IllegalArgumentException(
                        "Received event of unexpected command type: " + message.getOperation());
        }
    }

    @Override
    protected Object[] getOldColumnValues() {
        switch (getOperation()) {
            case CREATE:
            case TRUNCATE:
                return null;
            default:
                // UPDATE and DELETE have old values
                return VitessChangeRecordUtil.columnValues(connectorConfig, schema, message.getOldTupleList(), tableId);
        }
    }

    @Override
    protected Object[] getNewColumnValues() {
        switch (getOperation()) {
            case CREATE:
            case UPDATE:
                return VitessChangeRecordUtil.columnValues(connectorConfig, schema, message.getNewTupleList(), tableId);
            default:
                // DELETE does not have new values
                return null;
        }
    }

    @Override
    protected void emitTruncateRecord(Receiver receiver, TableSchema tableSchema) throws InterruptedException {
        Struct envelope = tableSchema.getEnvelopeSchema().truncate(getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), tableSchema, Envelope.Operation.TRUNCATE, null, envelope, getOffset(), null);
    }
}
