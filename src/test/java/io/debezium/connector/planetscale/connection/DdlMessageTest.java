/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.planetscale.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.Test;

import io.debezium.schema.SchemaChangeEvent;

/**
 * Verify the behavior of {@link DdlMessage}
 *
 * @author Thomas Thornton
 */
public class DdlMessageTest {

    @Test
    public void shouldSetQuery() {
        String statement = "ALTER TABLE foo RENAME TO bar";
        String keyspace = "keyspace";
        String shard = "-80";
        String tableName = "foo";
        DdlMessage ddlMessage = new DdlMessage("gtid", Instant.EPOCH, tableName, statement, keyspace, shard, SchemaChangeEvent.SchemaChangeEventType.ALTER);
        assertThat(ddlMessage.getStatement()).isEqualTo(statement);
    }

    @Test
    public void shouldSetShard() {
        String statement = "ALTER TABLE foo RENAME TO bar";
        String shard = "-80";
        String keyspace = "keyspace";
        String tableName = "foo";
        DdlMessage ddlMessage = new DdlMessage("gtid", Instant.EPOCH, tableName, statement, keyspace, shard, SchemaChangeEvent.SchemaChangeEventType.ALTER);
        assertThat(ddlMessage.getShard()).isEqualTo(shard);
    }

    @Test
    public void shouldSetKeyspace() {
        String statement = "ALTER TABLE foo RENAME TO bar";
        String shard = "-80";
        String keyspace = "keyspace";
        String tableName = "foo";
        DdlMessage ddlMessage = new DdlMessage("gtid", Instant.EPOCH, tableName, statement, keyspace, shard, SchemaChangeEvent.SchemaChangeEventType.ALTER);
        assertThat(ddlMessage.getKeyspace()).isEqualTo(keyspace);
    }

    @Test
    public void shouldConvertToString() {
        String statement = "ALTER TABLE foo RENAME TO bar";
        String shard = "-80";
        String keyspace = "keyspace";
        String tableName = "foo";
        ReplicationMessage replicationMessage = new DdlMessage("gtid", Instant.EPOCH, tableName, statement, keyspace, shard,
                SchemaChangeEvent.SchemaChangeEventType.ALTER);
        assertThat(replicationMessage.toString()).isEqualTo(
                "DdlMessage{transactionId='gtid', keyspace=keyspace, shard=-80, commitTime=1970-01-01T00:00:00Z, statement=ALTER TABLE foo RENAME TO bar, operation=DDL}");
    }

}
