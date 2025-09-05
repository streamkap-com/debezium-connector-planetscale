/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.planetscale.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;

import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.planetscale.TestHelper;
import io.debezium.connector.planetscale.Vgtid;
import io.debezium.connector.planetscale.VgtidTest;
import io.debezium.connector.planetscale.VitessConnectorConfig;
import io.debezium.connector.planetscale.VitessOffsetContext;
import io.debezium.connector.planetscale.VitessSchemaFactory;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;

public class VitessOrderedTransactionStructMakerTest {

    @Test
    public void prepareTxStruct() {
        VitessConnectorConfig config = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        VitessOrderedTransactionStructMaker maker = new VitessOrderedTransactionStructMaker(Configuration.empty());
        TransactionContext transactionContext = VitessOrderedTransactionContext.initialize(config);
        transactionContext.beginTransaction(new VitessTransactionInfo(VgtidTest.VGTID_JSON, VgtidTest.TEST_SHARD));
        OffsetContext context = new VitessOffsetContext(false, false, config, Vgtid.of(VgtidTest.VGTID_JSON), Instant.now(), transactionContext);
        Struct struct = maker.addTransactionBlock(context, 0, null);
        assertThat(struct.get(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH)).isEqualTo(0L);
        assertThat(struct.get(VitessOrderedTransactionContext.OFFSET_TRANSACTION_RANK)).isEqualTo(new BigDecimal(1513));
    }

    @Test
    public void getTransactionBlockSchema() {
        VitessOrderedTransactionStructMaker maker = new VitessOrderedTransactionStructMaker(Configuration.empty());
        assertThat(maker.getTransactionBlockSchema()).isEqualTo(VitessSchemaFactory.get().getOrderedTransactionBlockSchema());
    }
}
