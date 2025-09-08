/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.planetscale.connection;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.planetscale.VStreamCopyCompletedEventException;
import io.debezium.connector.planetscale.Vgtid;
import io.debezium.connector.planetscale.VitessConnector;
import io.debezium.connector.planetscale.VitessConnectorConfig;
import io.debezium.connector.planetscale.VitessConnectorConfig.SnapshotMode;
import io.debezium.connector.planetscale.VitessDatabaseSchema;
import io.debezium.connector.planetscale.VitessMetadata;
import io.debezium.connector.planetscale.VitessOffsetContext;
import io.debezium.connector.planetscale.pipeline.txmetadata.ShardEpochMap;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Strings;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.vitess.client.Proto;
import io.vitess.client.grpc.StaticAuthCredentials;
import io.vitess.proto.Topodata;
import io.vitess.proto.Vtgate;
import io.vitess.proto.Vtgate.VStreamRequest;
import io.vitess.proto.grpc.VitessGrpc;

import binlogdata.Binlogdata;
import binlogdata.Binlogdata.VEvent;

/**
 * Connection to VTGate to replication messages. Also connect to VTCtld to get the latest {@link
 * Vgtid} if no previous offset exists.
 */
public class VitessReplicationConnection implements ReplicationConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessReplicationConnection.class);

    private final MessageDecoder messageDecoder;
    private final VitessConnectorConfig config;
    // Channel closing is invoked from the change-event-source-coordinator thread
    private final AtomicReference<ManagedChannel> managedChannel = new AtomicReference<>();
    private final VitessMetadata vitessMetadata;

    public VitessReplicationConnection(VitessConnectorConfig config, VitessDatabaseSchema schema) {
        this.messageDecoder = new VStreamOutputMessageDecoder(schema, config.ddlFilter());
        this.config = config;
        this.vitessMetadata = new VitessMetadata(config);
    }

    /**
     * Execute SQL statement via vtgate gRPC.
     * @param sqlStatement The SQL statement to be executed
     * @throws StatusRuntimeException if the connection is not valid, or SQL statement can not be successfully exected
     */
    public Vtgate.ExecuteResponse execute(String sqlStatement) {
        ManagedChannel channel = newChannel();
        managedChannel.compareAndSet(null, channel);

        Vtgate.ExecuteRequest request = Vtgate.ExecuteRequest.newBuilder()
                .setQuery(Proto.bindQuery(sqlStatement, Collections.emptyMap()))
                .build();
        return newBlockingStub(channel).execute(request);
    }

    public Vtgate.ExecuteResponse execute(String sqlStatement, String shard) {
        ManagedChannel channel = newChannel();

        String target = String.format("%s:%s@%s", config.getKeyspace(), shard, config.getTabletType());
        Vtgate.Session session = Vtgate.Session.newBuilder().setTargetString(target).setAutocommit(true).build();
        LOGGER.debug("Autocommit {}", session.getAutocommit());
        Vtgate.ExecuteRequest request = Vtgate.ExecuteRequest.newBuilder()
                .setQuery(Proto.bindQuery(sqlStatement, Collections.emptyMap()))
                .setSession(session)
                .build();
        return newBlockingStub(channel).execute(request);
    }

    @Override
    public void startStreaming(
                               VitessOffsetContext offsetContext, ReplicationMessageProcessor processor, AtomicReference<Throwable> error) {
        Vgtid vgtid = offsetContext.getRestartVgtid();
        Objects.requireNonNull(vgtid);

        ManagedChannel channel = newChannel();
        managedChannel.compareAndSet(null, channel);

        VitessGrpc.VitessStub stub = newStub(channel);

        Map<String, String> grpcHeaders = config.getGrpcHeaders();
        if (!grpcHeaders.isEmpty()) {
            LOGGER.info("Setting VStream gRPC headers: {}", grpcHeaders);
            Metadata metadata = new Metadata();
            for (Map.Entry<String, String> entry : grpcHeaders.entrySet()) {
                metadata.put(Metadata.Key.of(entry.getKey(), Metadata.ASCII_STRING_MARSHALLER), entry.getValue());
            }
            stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata)); // MetadataUtils.attachHeaders(stub, metadata);
        }

        final Instant startedSnapshotAt;
        if (config.getSnapshotMode() == SnapshotMode.INITIAL_ONLY) {
            startedSnapshotAt = vitessMetadata.getCurrentTimestamp(config);
        }
        else {
            startedSnapshotAt = null;
        }

        StreamObserver<Vtgate.VStreamResponse> responseObserver = new ClientResponseObserver<VStreamRequest, Vtgate.VStreamResponse>() {
            private ClientCallStreamObserver<VStreamRequest> requestStream;
            private List<VEvent> bufferedEvents = new ArrayList<>();
            private Vgtid newVgtid;
            private boolean beginEventSeen;
            private boolean commitEventSeen;
            private int numOfRowEvents;
            private int numResponses;
            private boolean copyCompletedEventSeen;
            private boolean isInVStreamCopy = vgtid.willTriggerVStreamCopy();

            @Override
            public void onNext(Vtgate.VStreamResponse response) {
                LOGGER.debug("Received {} VEvents in the VStreamResponse:",
                        response.getEventsCount());
                boolean sendNow = false;
                boolean heartbeatReceived = false;
                for (VEvent event : response.getEventsList()) {
                    LOGGER.debug("VEvent: {}", event);
                    switch (event.getType()) {
                        case ROW:
                            numOfRowEvents++;
                            break;
                        case VGTID:
                            // We always use the latest VGTID if any.
                            if (newVgtid != null) {
                                if (newVgtid.getRawVgtid().getShardGtidsList().stream().findFirst().map(s -> s.getTablePKsCount() == 0).orElse(false)
                                        && event.getVgtid().getShardGtidsList().stream().findFirst().map(s -> 0 < s.getTablePKsCount()).orElse(false)) {
                                    LOGGER.info("Received more than one VGTID events during a copy operation and the previous one is {}. Using the latest: {}",
                                            newVgtid.toString(),
                                            event.getVgtid().toString());
                                }
                                else {
                                    LOGGER.warn("Received more than one VGTID events and the previous one is {}. Using the latest: {}",
                                            newVgtid.toString(),
                                            event.getVgtid().toString());
                                }
                            }
                            newVgtid = Vgtid.of(event.getVgtid());
                            break;
                        case BEGIN:
                            // We should only see BEGIN before seeing COMMIT.
                            if (commitEventSeen) {
                                String msg = "Received BEGIN event after receiving COMMIT event";
                                setError(msg);
                                return;
                            }
                            if (beginEventSeen) {
                                String msg = "Received duplicate BEGIN events";
                                // During a copy operation, we receive the duplicate event once when no record is copied.
                                String eventTypes = bufferedEvents.stream().map(VEvent::getType).map(Objects::toString).collect(Collectors.joining(", "));
                                if (eventTypes.equals("BEGIN, FIELD") || eventTypes.equals("BEGIN, FIELD, VGTID") || eventTypes.equals("COPY_COMPLETED, BEGIN, FIELD")) {
                                    msg += String.format(" during a copy operation. No harm to skip the buffered events. Buffered event types: %s",
                                            eventTypes);
                                    LOGGER.info(msg);
                                    reset();
                                }
                                else {
                                    setError(msg);
                                    return;
                                }
                            }
                            beginEventSeen = true;
                            break;
                        case COMMIT:
                            // We should only see COMMIT after seeing BEGIN.
                            if (!beginEventSeen) {
                                String msg = "Received COMMIT event before receiving BEGIN event";
                                setError(msg);
                                return;
                            }
                            if (commitEventSeen) {
                                String msg = "Received duplicate COMMIT events";
                                setError(msg);
                                return;
                            }
                            commitEventSeen = true;
                            messageDecoder.setCommitTimestamp(Instant.ofEpochSecond(event.getTimestamp()));
                            break;
                        case COPY_COMPLETED:
                            // After all shards are copied, Vitess will send a final COPY_COMPLETED event.
                            // See:
                            // https://github.com/vitessio/vitess/blob/v19.0.0/go/vt/vtgate/vstream_manager.go#L791-L808
                            if (event.getKeyspace() == "" && event.getShard() == "") {
                                LOGGER.info("Received COPY_COMPLETED event for all keyspaces and shards");
                                offsetContext.markSnapshotRecord(SnapshotRecord.FALSE);
                                copyCompletedEventSeen = true;
                            }
                            else {
                                LOGGER.info("Received COPY_COMPLETED event for keyspace {} and shard {}",
                                        event.getKeyspace(), event.getShard());
                            }
                            continue;
                        case DDL:
                        case OTHER:
                            // If receiving DDL and OTHER, process them immediately to rotate vgtid in offset.
                            // For example, the response can be:
                            // [VGTID, DDL]. This is an DDL event.
                            // [VGTID, OTHER]. This is the first response if "current" is used as starting gtid.
                            sendNow = true;
                            break;
                        case HEARTBEAT:
                            heartbeatReceived = true;
                            // Mark sendNow true since begin/commit events may not have been received for just heartbeat events
                            sendNow = true;
                            break;
                    }
                    bufferedEvents.add(event);
                }

                numResponses++;

                // We only proceed when we receive a complete transaction after seeing both BEGIN and COMMIT events,
                // OR if sendNow flag is true (meaning we should process buffered events immediately).
                if ((!beginEventSeen || !commitEventSeen) && !sendNow && !copyCompletedEventSeen) {
                    LOGGER.debug("Received partial transaction: number of responses so far is {}", numResponses);
                    return;
                }
                if (numResponses > 1) {
                    LOGGER.debug("Processing multi-response transaction: number of responses is {}", numResponses);
                }
                // If there is a heartbeat event we do not want to skip (we want to send the heartbeat)
                if (newVgtid == null && !heartbeatReceived && !copyCompletedEventSeen) {
                    LOGGER.warn("Skipping because no vgtid is found in buffered event types: {}",
                            bufferedEvents.stream().map(VEvent::getType).map(Objects::toString).collect(Collectors.joining(", ")));
                    reset();
                    return;
                }

                // Send the buffered events that belong to the same transaction.
                try {
                    int rowEventSeen = 0;
                    for (int i = 0; i < bufferedEvents.size(); i++) {
                        VEvent event = bufferedEvents.get(i);
                        if (event.getType() == Binlogdata.VEventType.ROW) {
                            rowEventSeen++;
                        }
                        if (isInVStreamCopy && event.getType() == Binlogdata.VEventType.COPY_COMPLETED) {
                            isInVStreamCopy = false;
                        }
                        messageDecoder.processMessage(bufferedEvents.get(i), processor, newVgtid, isInVStreamCopy, offsetContext.isInitialSnapshotRunning(),
                                startedSnapshotAt);
                    }
                }
                catch (InterruptedException e) {
                    LOGGER.error("Message processing is interrupted", e);
                    // Only propagate the first error
                    error.compareAndSet(null, e);
                    Thread.currentThread().interrupt();
                }
                finally {
                    reset();
                }

                if (copyCompletedEventSeen) {
                    LOGGER.info("Received COPY_COMPLETED event for all keyspaces and shards");
                    if (config.getSnapshotMode() == SnapshotMode.INITIAL_ONLY) {
                        LOGGER.info("Cancel the copy operation after receiving COPY_COMPLETED event");
                        requestStream.cancel("Cancel the copy operation after receiving COPY_COMPLETED event",
                                new VStreamCopyCompletedEventException());
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                LOGGER.error("VStream streaming onError. Status: {}", Status.fromThrowable(t), t);
                // Only propagate the first error
                error.compareAndSet(null, t);
                reset();
            }

            @Override
            public void onCompleted() {
                LOGGER.info("VStream streaming completed.");
                reset();
            }

            private void reset() {
                bufferedEvents.clear();
                newVgtid = null;
                beginEventSeen = false;
                commitEventSeen = false;
                numOfRowEvents = 0;
                numResponses = 0;
            }

            /**
             * Create and set an error for error handler and reset.
             */
            private void setError(String msg) {
                msg += String.format(". Buffered event types: %s",
                        bufferedEvents.stream().map(VEvent::getType).map(Objects::toString).collect(Collectors.joining(", ")));
                LOGGER.error(msg);
                error.compareAndSet(null, new DebeziumException(msg));
                reset();
            }

            @Override
            public void beforeStart(ClientCallStreamObserver<VStreamRequest> requestStream) {
                this.requestStream = requestStream;
            }
        };

        Vtgate.VStreamFlags.Builder flagBuilder = Vtgate.VStreamFlags.newBuilder()
                .setStopOnReshard(config.getStopOnReshard())
                .setHeartbeatInterval(getHeartbeatSeconds())
                .setStreamKeyspaceHeartbeats(config.getStreamKeyspaceHeartbeats());

        String cells = config.getCells();
        if (!Strings.isNullOrEmpty(cells)) {
            flagBuilder = flagBuilder.setCells(cells);
        }

        Vtgate.VStreamFlags vStreamFlags = flagBuilder.build();

        final Map<String, String> tableSQL = new HashMap<String, String>();

        if (!Strings.isNullOrEmpty(config.tableIncludeList()) || config.isColumnsFiltered()) {
            List<String> tables = vitessMetadata.getKeyspaceTables(config);
            LOGGER.info("Found tables in keyspace: {}.", Strings.join(",", tables));

            if (!Strings.isNullOrEmpty(config.tableIncludeList())) {
                tables = VitessConnector.getIncludedTables(config, tables);
                LOGGER.info("Using only tables included in table include list: {}.", Strings.join(",", tables));
            }
            for (String table : tables) {
                String sql = "select * from `" + table + "`";
                if (config.isColumnsFiltered()) {
                    List<String> allColumns = vitessMetadata.getTableColumns(config, table);
                    List<String> includedColumns = vitessMetadata.getColumnsForTable(config.getKeyspace(),
                            config.getColumnFilter(), allColumns, table);
                    sql = String.format("select %s from `%s`", String.join(",", includedColumns), table);
                    List<String> escapedColumns = new ArrayList<String>();
                    for (String includedColumn : includedColumns) {
                        escapedColumns.add(String.format("`%s`", includedColumn));
                    }
                    sql = String.format("select %s from `%s`", String.join(",", escapedColumns), table);
                }
                tableSQL.put(table, sql);
            }
        }
        // Providing a vgtid MySQL56/19eb2657-abc2-11ea-8ffc-0242ac11000a:1-61 here will make VStream to
        // start receiving row-changes from MySQL56/19eb2657-abc2-11ea-8ffc-0242ac11000a:1-62

        Map<DataCollectionId, String> selectOverrides = config.getSnapshotSelectOverridesByTable();
        if (!selectOverrides.isEmpty()) {
            selectOverrides.forEach((dataCollectionId, selectOverride) -> {
                TableId tableId = (TableId) dataCollectionId;
                tableSQL.put(tableId.table(), selectOverride);
            });
        }

        Binlogdata.Filter.Builder filterBuilder = Binlogdata.Filter.newBuilder();
        for (Map.Entry<String, String> entry : tableSQL.entrySet()) {
            String table = entry.getKey();
            String sql = entry.getValue();
            LOGGER.info("Running Sql Query: {}", sql);

            // See rule in:
            // https://github.com/vitessio/vitess/blob/release-14.0/go/vt/vttablet/tabletserver/vstreamer/planbuilder.go#L316
            Binlogdata.Rule rule = Binlogdata.Rule.newBuilder().setMatch(table).setFilter(sql).build();
            LOGGER.info("Add vstream table filtering: {}", rule.getMatch());
            filterBuilder.addRules(rule);
        }

        // Providing a vgtid MySQL56/19eb2657-abc2-11ea-8ffc-0242ac11000a:1-61 here will
        // make VStream to
        // start receiving row-changes from
        // MySQL56/19eb2657-abc2-11ea-8ffc-0242ac11000a:1-62
        VStreamRequest.Builder vstreamBuilder = VStreamRequest.newBuilder()
                .setVgtid(vgtid.getRawVgtid())
                .setTabletType(
                        toTopodataTabletType(VitessTabletType.valueOf(config.getTabletType())))
                .setFlags(vStreamFlags);
        if (filterBuilder.getRulesCount() > 0) {
            vstreamBuilder.setFilter(filterBuilder);
        }
        if (config.getSnapshotMode() != SnapshotMode.NEVER) {
            LOGGER.info("Starting VStream with snapshot mode: {}", config.getSnapshotMode());
        }
        else {
            LOGGER.info("Starting VStream without snapshot");
        }
        VStreamRequest request = vstreamBuilder.build();
        stub.vStream(
                request,
                responseObserver);
        LOGGER.info("Started VStream {}", request);
    }

    private int getHeartbeatSeconds() {
        long secondsLong = config.getHeartbeatInterval().toSeconds();
        if (secondsLong > Integer.MAX_VALUE) {
            LOGGER.warn("Heartbeat interval {} seconds exceeds the maximum value of integer, using max value", secondsLong);
            return Integer.MAX_VALUE;
        }
        else {
            return (int) secondsLong;
        }
    }

    private VitessGrpc.VitessStub newStub(ManagedChannel channel) {
        VitessGrpc.VitessStub stub = VitessGrpc.newStub(channel);
        return withBasicAuthentication(stub);
    }

    private VitessGrpc.VitessBlockingStub newBlockingStub(ManagedChannel channel) {
        VitessGrpc.VitessBlockingStub stub = VitessGrpc.newBlockingStub(channel);
        return withBasicAuthentication(stub);
    }

    private <T extends AbstractStub<T>> T withCredentials(T stub) {
        if (config.getVtgateUsername() != null && config.getVtgatePassword() != null) {
            LOGGER.info("Use authenticated vtgate grpc.");
            stub = stub.withCallCredentials(new StaticAuthCredentials(config.getVtgateUsername(), config.getVtgatePassword()));
        }
        return stub;
    }

    private <T extends AbstractStub<T>> T withBasicAuthentication(T stub) {
        BasicAuthenticationInterceptor authInterceptor = config.getBasicAuthenticationInterceptor();
        if (authInterceptor != null) {
            LOGGER.info("Use Basic authentication to vtgate grpc.");
            stub = stub.withInterceptors(authInterceptor);
        }
        return stub;
    }

    private ManagedChannel newChannel() {
        ChannelCredentials tlsChannelCredentials = config.getTLSChannelCredentials();
        ManagedChannelBuilder channelBuilder;
        if (tlsChannelCredentials == null) {
            LOGGER.info("Use plainText connection to vtgate grpc.");
            channelBuilder = ManagedChannelBuilder.forAddress(config.getVtgateHost(), config.getVtgatePort())
                    .usePlaintext();
        }
        else {
            LOGGER.info("Use TLS connection to vtgate grpc.");
            //channelBuilder = Grpc.newChannelBuilderForAddress(config.getVtgateHost(), config.getVtgatePort(), tlsChannelCredentials);
            channelBuilder = NettyChannelBuilder.forAddress(config.getVtgateHost(), config.getVtgatePort(),tlsChannelCredentials);
        }
        channelBuilder = channelBuilder.defaultLoadBalancingPolicy(config.getGrpcDefaultLoadBalancingPolicy())
                .maxInboundMessageSize(config.getGrpcMaxInboundMessageSize())
                .keepAliveTime(config.getKeepaliveInterval().toMillis(), TimeUnit.MILLISECONDS);
        if (tlsChannelCredentials == null) {
            channelBuilder = channelBuilder.usePlaintext();
        }

        return channelBuilder.build();
    }

    /** Close the gRPC connection to VStream */
    @Override
    public void close() throws Exception {
        LOGGER.info("Closing replication connection");
        managedChannel.get().shutdownNow();
        LOGGER.trace("VStream GRPC channel shutdownNow is invoked.");
        if (managedChannel.get().awaitTermination(5, TimeUnit.SECONDS)) {
            LOGGER.info("VStream GRPC channel is shutdown in time.");
        }
        else {
            LOGGER.warn("VStream GRPC channel is not shutdown in time. Give up waiting.");
        }
    }

    public static Vgtid buildVgtid(String keyspace, List<String> shards, List<String> gtids) {
        Binlogdata.VGtid.Builder builder = Binlogdata.VGtid.newBuilder();
        Vgtid vgtid;
        if (shards == null || shards.isEmpty()) {
            vgtid = Vgtid.of(builder.addShardGtids(
                    Binlogdata.ShardGtid.newBuilder()
                            .setKeyspace(keyspace)
                            .setGtid(Vgtid.CURRENT_GTID)
                            .build())
                    .build());
        }
        else {
            for (int i = 0; i < shards.size(); i++) {
                String shard = shards.get(i);
                String gtid = gtids.get(i);
                builder.addShardGtids(
                        Binlogdata.ShardGtid.newBuilder()
                                .setKeyspace(keyspace)
                                .setShard(shard)
                                .setGtid(gtid)
                                .build());
            }
            vgtid = Vgtid.of(builder.build());
        }
        LOGGER.info("Default VGTID '{}' for keyspace {}, shards: {}, gtids {}", vgtid, keyspace, shards, gtids);
        return vgtid;
    }

    /**
     * Get latest replication position. If offset storage mode is enabled, then read the vgtid that was set
     * by {@link io.debezium.connector.planetscale.VitessConnectorTask#getConfigWithOffsets} for the task vgtid
     * property. If not then read it from the configs or get the shards from Vitess to initialize.
     *
     * @param config
     * @return default vgtid
     */
    public static Vgtid defaultVgtid(VitessConnectorConfig config) {
        Vgtid vgtid;
        if (config.offsetStoragePerTask()) {
            List<String> shards = config.getVitessTaskKeyShards();
            vgtid = config.getVitessTaskVgtid();
            LOGGER.info("VGTID is set for the keyspace: {}, shards: {}, vgtid: {}",
                    config.getKeyspace(), shards, vgtid);
        }
        else {
            // If offset storage per task is disabled, then find the vgtid elsewhere
            if (config.getShard() == null || config.getShard().isEmpty()) {
                // This case is not supported by the Vitess, so our workaround is to get all the shards from vtgate.
                if (config.getVgtid() == Vgtid.EMPTY_GTID) {
                    List<String> shards = new VitessMetadata(config).getShards();
                    List<String> gtids = Collections.nCopies(shards.size(), config.getVgtid());
                    vgtid = buildVgtid(config.getKeyspace(), shards, gtids);
                }
                else {
                    // Passing in empty lists will result in "CURRENT" set for all shards
                    vgtid = buildVgtid(config.getKeyspace(), Collections.emptyList(), Collections.emptyList());
                }
                LOGGER.info("Default VGTID '{}' is set to the current gtid of all shards from keyspace: {}",
                        vgtid, config.getKeyspace());
            }
            else {
                // There is a shard specified in the config
                List<String> shards = config.getShard();
                String vgtidString = config.getVgtid();
                List<String> gtids;
                if (vgtidString == Vgtid.CURRENT_GTID ||
                        vgtidString == Vgtid.EMPTY_GTID) {
                    gtids = Collections.nCopies(shards.size(), vgtidString);
                    vgtid = buildVgtid(config.getKeyspace(), shards, gtids);
                }
                else {
                    // If it's not current or empty, then it must be an actual vgtid
                    vgtid = Vgtid.of(vgtidString);
                }
                LOGGER.info("VGTID '{}' is set to the GTID {} for keyspace: {} shard: {}",
                        vgtid, vgtidString, config.getKeyspace(), shards);
            }
        }
        return vgtid;
    }

    /**
     * Get latest shard epoch map. If offset storage mode is enabled, then read the epoch that was set
     * by {@link io.debezium.connector.planetscale.VitessConnectorTask#getConfigWithOffsets} for the task shard epoch map
     * property. If not then read it from the configs or get the shards from Vitess to initialize.
     *
     * @param config
     * @return
     */
    public static ShardEpochMap defaultShardEpochMap(VitessConnectorConfig config) {
        ShardEpochMap shardEpochMap;
        if (config.offsetStoragePerTask()) {
            // The epoch values are read or initialized to zero in VitessConnectorTask
            shardEpochMap = config.getVitessTaskShardEpochMap();
            LOGGER.info("ShardEpochMap '{}' is set for the keyspace: {}",
                    shardEpochMap, config.getKeyspace());
        }
        else {
            if (!config.getShardEpochMap().isEmpty()) {
                shardEpochMap = ShardEpochMap.of(config.getShardEpochMap());
            }
            else if (config.getShard() == null || config.getShard().isEmpty()) {
                List<String> shards = new VitessMetadata(config).getShards();
                shardEpochMap = ShardEpochMap.init(shards);
            }
            else {
                List<String> shards = config.getShard();
                shardEpochMap = ShardEpochMap.init(shards);
            }
        }
        return shardEpochMap;
    }

    private static Topodata.TabletType toTopodataTabletType(VitessTabletType tabletType) {
        switch (tabletType) {
            case MASTER:
                return Topodata.TabletType.MASTER;
            case REPLICA:
                return Topodata.TabletType.REPLICA;
            case RDONLY:
                return Topodata.TabletType.RDONLY;
            default:
                LOGGER.warn("Unknown tabletType {}", tabletType);
                return null;
        }
    }
}
