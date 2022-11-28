// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.external.iceberg;

import com.starrocks.common.Config;
import com.starrocks.common.Status;
import com.starrocks.common.TimeoutException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.thrift.TStatusCode;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FindFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.orc.OrcMetrics;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;

public class IcebergFilesMgr {
    static long MaxCommitRetries = 10;

    static Configuration conf = new Configuration();
    private static final Logger LOG = LogManager.getLogger(IcebergFilesMgr.class);
    private final Table icebergTable;

    private final Schema schema;
    private final PartitionSpec partitionSpec;
    private final boolean isIcebergTablePartitioned;

    public IcebergFilesMgr(Table icebergTable) {
        this.icebergTable = icebergTable;
        this.isIcebergTablePartitioned = !icebergTable.spec().isUnpartitioned();
        this.schema = icebergTable.schema();
        this.partitionSpec = icebergTable.spec();
    }

    public static Configuration getHadoopConf() {
        return conf;
    }

    public synchronized Status addIcebergFiles(
            Set<String> appendFileNames, long start, long timeoutMs) {

        LOG.info("routine load add iceberg files start at: {} with timeout {}ms",
                TimeUtils.longToTimeString(start), timeoutMs);
        DeleteParam deleteParam = new DeleteParam(DeleteFlag.NoDelete);
        return addIcebergFiles(appendFileNames, deleteParam, start, timeoutMs);
    }

    // use DeleteFlag for a better commit performance, DeletePartition and DeleteAll case doesn't need get all data files
    public enum DeleteFlag {
        NoDelete,
        DeleteFiles,
        DeletePartition,
        DeleteAll,
    }

    public static class DeleteParam {
        public DeleteFlag deleteFlag;
        // need when deleteFlag is DeleteFiles
        public Expression deleteFilterExpression;
        // need when deleteFlag is DeletePartition
        public org.apache.iceberg.data.GenericRecord partitionValue;

        public DeleteParam(DeleteFlag deleteFlag) {
            this.deleteFlag = deleteFlag;
        }
    }

    // check timeout before real commit to iceberg
    private synchronized void checkTimeout(long start, long timeoutMs) throws TimeoutException {
        if (timeoutMs > 0 && ((System.currentTimeMillis() - start) > timeoutMs)) {
            String msg = String.format(
                    "add iceberg files failed, prepare commit exceed timeout %s", timeoutMs);
            throw new TimeoutException(msg);
        }
    }

    private synchronized void commitOnce(Set<DataFile> addFiles, DeleteParam deleteParam,
                                         long start, long timeoutMs) throws TimeoutException {
        LOG.info("commit to iceberg table {} with add[{}] files and delete:{}",
                icebergTable.name(), addFiles.size(), deleteParam);
        switch (deleteParam.deleteFlag) {
            case NoDelete:
                if (addFiles.size() > 0) {
                    AppendFiles appendFiles = icebergTable.newAppend();
                    for (DataFile df : addFiles) {
                        appendFiles.appendFile(df);
                    }
                    checkTimeout(start, timeoutMs);
                    appendFiles.commit();
                }
                break;
            case DeleteFiles:
                Iterable<DataFile> deleteFileIter = FindFiles.in(icebergTable).withRecordsMatching(
                        deleteParam.deleteFilterExpression).collect();
                Set<DataFile> deletedFiles = new HashSet<>();
                for (DataFile file : deleteFileIter) {
                    deletedFiles.add(file);
                }

                LOG.info("commit to iceberg table {} with delete[{}] and add[{}]",
                        icebergTable.name(), deletedFiles.size(), addFiles.size());
                if (deletedFiles.size() > 0 && addFiles.size() > 0) {
                    RewriteFiles rewriteFiles = icebergTable.newRewrite();
                    rewriteFiles.rewriteFiles(deletedFiles, addFiles);
                    checkTimeout(start, timeoutMs);
                    rewriteFiles.commit();
                } else if (addFiles.size() > 0) {
                    AppendFiles appendFiles = icebergTable.newAppend();
                    for (DataFile df : addFiles) {
                        appendFiles.appendFile(df);
                    }
                    checkTimeout(start, timeoutMs);
                    appendFiles.commit();
                } else if (deletedFiles.size() > 0) {
                    DeleteFiles deleteFiles = icebergTable.newDelete();
                    for (DataFile df : deletedFiles) {
                        deleteFiles.deleteFile(df);
                    }
                    checkTimeout(start, timeoutMs);
                    deleteFiles.commit();
                }
                break;
            case DeletePartition:
                if (addFiles.size() > 0) {
                    ReplacePartitions replacePartitions = icebergTable.newReplacePartitions();
                    for (DataFile df : addFiles) {
                        replacePartitions.addFile(df);
                    }
                    checkTimeout(start, timeoutMs);
                    replacePartitions.commit();
                } else {
                    DeleteFiles deleteFiles = icebergTable.newDelete();
                    Iterable<DataFile> fileIter = FindFiles.in(icebergTable).inPartition(
                            icebergTable.spec(), deleteParam.partitionValue).collect();
                    int counter = 0;
                    for (DataFile file : fileIter) {
                        deleteFiles.deleteFile(file);
                        counter += 1;
                    }
                    if (counter > 0) {
                        checkTimeout(start, timeoutMs);
                        deleteFiles.commit();
                    }
                }
                break;
            case DeleteAll:
                if (addFiles.size() > 0) {
                    ReplacePartitions replacePartitions = icebergTable.newReplacePartitions();
                    for (DataFile df : addFiles) {
                        replacePartitions.addFile(df);
                    }
                    checkTimeout(start, timeoutMs);
                    replacePartitions.commit();
                } else {
                    // delete all files
                    Iterable<DataFile> files = FindFiles.in(icebergTable).collect();
                    DeleteFiles deleteFiles = icebergTable.newDelete();
                    int counter = 0;
                    for (DataFile file : files) {
                        deleteFiles.deleteFile(file);
                        counter += 1;
                    }
                    if (counter > 0) {
                        checkTimeout(start, timeoutMs);
                        deleteFiles.commit();
                    }
                }
                break;
            default:
                // noop
        };
    }

    public synchronized Status addIcebergFiles(
            Set<String> appendFileNames, DeleteParam deleteParam, long start, long timeoutMs) {

        DataFile dataFile;
        Set<DataFile> addFiles = new HashSet<>();
        for (String filename : appendFileNames) {
            try {
                dataFile = buildFile(filename);
            } catch (Exception e) {
                LOG.error("read orc metrics failed", e);
                return new Status(TStatusCode.CANCELLED, String.format("read orc metrics failed, %s", e));
            }
            addFiles.add(dataFile);
        }

        LOG.info("begin commit to iceberg files, count:{}", appendFileNames);
        if (timeoutMs > 0 && ((System.currentTimeMillis() - start) > timeoutMs)) {
            LOG.warn("add iceberg files failed, exceed timeout {}", timeoutMs);
            String msg = String.format("add iceberg files failed, exceed timeout %s", timeoutMs);
            return new Status(TStatusCode.CANCELLED, msg);
        }

        long maxRetries = Config.iceberg_commit_max_retries;
        maxRetries = Math.max(maxRetries, 1);
        maxRetries = Math.min(maxRetries, MaxCommitRetries);
        long retries = 1;

        boolean commitSuccess = false;
        while (!commitSuccess) {
            try {
                commitOnce(addFiles, deleteParam, start, timeoutMs);
                commitSuccess = true;
            } catch (TimeoutException e) {
                return new Status(TStatusCode.CANCELLED, e.getMessage());
            } catch (CommitFailedException e) {
                retries += 1;
                if (retries > maxRetries) {
                    LOG.error("commit iceberg files {} failed, {}", appendFileNames, e);
                    return new Status(TStatusCode.CANCELLED, String.format("iceberg commit failed, %s", e));
                }

                LOG.warn("commit iceberg files {} failed, retry {}", appendFileNames, retries);
            }
        }

        LOG.info("finished commit to iceberg files {}", appendFileNames);
        return Status.OK;
    }

    private DataFile buildFile(String filePath)
            throws Exception {

        String filePrefix = icebergTable.location() + "/data/";
        if (!filePath.startsWith(filePrefix)) {
            String msg = String.format(
                    "invalid file location [%s], should prefix with iceberg table location", filePath);
            throw new Exception(msg);
        }

        HadoopInputFile inputFile = HadoopInputFile.fromLocation(filePath, conf);
        NameMapping nameMapping = MappingUtil.create(schema);
        // todo specified spec mode from request.
        Metrics metrics = OrcMetrics.fromInputFile(inputFile, MetricsConfig.getDefault(), nameMapping);
        DataFiles.Builder builder = DataFiles.builder(partitionSpec)
                                             .withPath(filePath)
                                             .withFormat(FileFormat.fromFileName(filePath))
                                             .withMetrics(metrics)
                                             .withFileSizeInBytes(inputFile.getLength());
        if (isIcebergTablePartitioned) {
            LOG.debug("partitioned table, add partition path");
            String appendix = filePath.replace(filePrefix, "");
            String partitionPath = Paths.get(appendix).getParent().getFileName().toString();
            partitionPath = StringUtils.strip(partitionPath, File.separator);
            LOG.debug("got partition key {} from filepath {}", partitionPath, filePath);

            PartitionKey partitionKey  = new PartitionKey(partitionSpec, schema);
            fillFromPath(partitionSpec, partitionPath, partitionKey);
            builder = builder.withPartition(partitionKey);
        }
        return builder.build();
    }

    private void fillFromPath(PartitionSpec spec, String partitionPath, PartitionKey partitionKey) {

        String[] partitions = partitionPath.split("/", -1);
        Preconditions.checkArgument(partitions.length <= spec.fields().size(),
                "Invalid partition data, too many fields (expecting %s): %s", spec.fields().size(), partitionPath);
        Preconditions.checkArgument(partitions.length >= spec.fields().size(),
                "Invalid partition data, not enough fields (expecting %s): %s", spec.fields().size(), partitionPath);

        for (int i = 0; i < partitions.length; ++i) {
            PartitionField field = spec.fields().get(i);
            String[] parts = partitions[i].split("=", 2);
            Preconditions.checkArgument(parts.length == 2 && parts[0] != null && field.name().equals(parts[0]),
                    "Invalid partition: %s", partitions[i]);
            Type type = spec.partitionType().fields().get(i).type();
            if (type.typeId() == Type.TypeID.TIMESTAMP) {
                String timestampStr = parts[1];
                timestampStr = timestampStr.replaceAll("%3A", ":");
                if (timestampStr.endsWith("Z")) {
                    timestampStr = timestampStr.substring(0, timestampStr.length() - 1);
                }
                LocalDateTime timestamp = LocalDateTime.parse(timestampStr);
                partitionKey.set(i, timestamp.toInstant(ZoneOffset.UTC).toEpochMilli() * 1000);

            } else {
                partitionKey.set(i, Conversions.fromPartitionString(type, parts[1]));
            }

        }
    }
}
