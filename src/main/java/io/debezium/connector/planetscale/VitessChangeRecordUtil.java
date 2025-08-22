/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.planetscale;

import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.planetscale.connection.ReplicationMessage;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Strings;

final class VitessChangeRecordUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessChangeRecordUtil.class);

    static Object[] columnValues(VitessConnectorConfig connectorConfig, VitessDatabaseSchema schema, List<ReplicationMessage.Column> columns, TableId tableId) {
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        final Table table = schema.tableFor(tableId);
        Objects.requireNonNull(table);

        Object[] values = new Object[columns.size()];
        for (ReplicationMessage.Column column : columns) {
            final String columnName = Strings.unquoteIdentifierPart(column.getName());
            int position = getPosition(columnName, table, values.length);
            if (position != -1) {
                Object value = column.getValue(connectorConfig.includeUnknownDatatypes());
                values[position] = value;
            }
            else {
                LOGGER.error("Can not find position for {} in {}", columnName, table);
            }
        }
        return values;
    }

    static int getPosition(String columnName, Table table, int maxPosition) {
        final Column tableColumn = table.columnWithName(columnName);
        if (tableColumn == null) {
            LOGGER.warn(
                    "Internal schema is out-of-sync with incoming decoder events; column {} will be omitted from the change event.",
                    columnName);
            return -1;
        }
        int position = tableColumn.position() - 1;
        if (position < 0 || position >= maxPosition) {
            LOGGER.warn(
                    "Internal schema is out-of-sync with incoming decoder events; column {} will be omitted from the change event.",
                    columnName);
            return -1;
        }
        return position;
    }
}
