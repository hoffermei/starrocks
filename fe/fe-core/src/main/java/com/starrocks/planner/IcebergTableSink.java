// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TIcebergTableSink;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.system.SystemInfoService.DEFAULT_CLUSTER;

public class IcebergTableSink extends DataSink {
    private IcebergTable icebergTable;
    private final String db;
    private final String table;
    private TupleDescriptor tupleDescriptor;
    private final String location;
    private final String fileFormat;
    private final List<String> sortFields;
    private final BrokerDesc brokerDesc;
    private final Long rowsPerFile;
    private final Long bytesPerFile;
    private String fileNamePrefix;
    // set after init called
    private TDataSink tDataSink;

    public IcebergTableSink(IcebergTable icebergTable, TupleDescriptor tupleDescriptor, BrokerDesc brokerDesc) {
        this.icebergTable = icebergTable;
        db = icebergTable.getDb();
        table = icebergTable.getTable();
        this.tupleDescriptor = tupleDescriptor;
        org.apache.iceberg.Table table = icebergTable.getIcebergTable();
        location = table.location();
        sortFields = table.sortOrder().fields().stream().map(f -> f.toString()).collect(Collectors.toList());
        fileFormat = table.properties().getOrDefault("write.format.default", "orc").toLowerCase();
        rowsPerFile = 1000000L;
        bytesPerFile = 134217728L;
        fileNamePrefix = this.table;
        this.brokerDesc = brokerDesc;
    }

    public void init(TUniqueId loadId, long txnId, long dbId, long loadChannelTimeoutS) throws AnalysisException {
        tDataSink = new TDataSink(TDataSinkType.ICEBERG_TABLE_SINK);
        TIcebergTableSink tIcebergTableSink = new TIcebergTableSink(
                loadId,
                dbId,
                icebergTable.getId(),
                tupleDescriptor.getId().asInt(),
                location, fileFormat, rowsPerFile, bytesPerFile, null);

        tIcebergTableSink.setLoad_id(loadId);
        tIcebergTableSink.setDb_id(dbId);
        tIcebergTableSink.setLoad_channel_timeout_s(loadChannelTimeoutS);

        FsBroker broker = GlobalStateMgr.getCurrentState().getBrokerMgr().getAnyBroker(brokerDesc.getName());
        if (broker != null) {
            tIcebergTableSink.addToBroker_addresses(new TNetworkAddress(broker.ip, broker.port));
        }
        tIcebergTableSink.setProperties(brokerDesc.getProperties());
        tIcebergTableSink.setTuple_id(tupleDescriptor.getId().asInt());
        if (fileNamePrefix != null) {
            tIcebergTableSink.setFile_name_prefix(fileNamePrefix);
        }
        if (sortFields.size() != 0) {
            tIcebergTableSink.setSort_fields(sortFields);
        }

        tDataSink.setIceberg_table_sink(tIcebergTableSink);
    }

    // must called after tupleDescriptor is computed
    public void complete() throws UserException {
        TIcebergTableSink tSink = tDataSink.getIceberg_table_sink();
        tSink.setTuple_id(tupleDescriptor.getId().asInt());
        tSink.setTableDescripter(icebergTable.toThrift(Lists.newArrayList()));
    }

    public String getFileNamePrefix() {
        return fileNamePrefix;
    }

    public void setFileNamePrefix(String fileNamePrefix) {
        this.fileNamePrefix = fileNamePrefix;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix + "ICEBERG TABLE SINK\n");
        sb.append(prefix + "  db=" + db + "\n");
        sb.append(prefix + "  table=" + table + "\n");
        sb.append(prefix + "  location="
                + StringEscapeUtils.escapeJava(location) + "\n");
        sb.append(prefix + "  fileFormat=" + StringEscapeUtils.escapeJava(fileFormat) + "\n");
        sb.append(prefix + "  rowsPerFile=" + rowsPerFile + "\n");
        sb.append(prefix + "  bytesPerFile=" + bytesPerFile + "\n");
        sb.append(prefix + "  fileNamePrefix=" + fileNamePrefix + "\n");
        sb.append(prefix + "  broker_name=" + brokerDesc.getName() + " property("
                + new PrintableMap<String, String>(
                brokerDesc.getProperties(), "=", true, false)
                + ")");
        sb.append("\n");
        return sb.toString();
    }

    @Override
    public TDataSink toThrift() {
        return tDataSink;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }
}
