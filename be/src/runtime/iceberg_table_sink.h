// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <vector>

#include "common/status.h"
#include "exec/data_sink.h"
#include "runtime/mysql_table_writer.h"
#include "descriptors.h"

namespace starrocks {

class RowDescriptor;
class TExpr;
class TIcebergTableSink;
class RuntimeState;
class RuntimeProfile;
class ExprContext;
class FileBuilder;

// This class is a sinker, which put input data to mysql table
class IcebergTableSink : public DataSink {
public:
    IcebergTableSink(ObjectPool* pool, const RowDescriptor& row_desc, const std::vector<TExpr>& t_exprs);

    ~IcebergTableSink() override;

    Status init(const TDataSink& thrift_sink) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status send_chunk(RuntimeState* state, vectorized::Chunk* chunk) override;

    // Flush all buffered data and close all existing channels to destination
    // hosts. Further send() calls are illegal after calling close().
    Status close(RuntimeState* state, Status exec_status) override;

    RuntimeProfile* profile() override { return _profile; }

private:
    Status prepare_partition_writer(const std::string& partition_key);

    Status gen_file_name(std::string* file_name);

    Status write_to_partition(vectorized::Chunk* chunk, const string& partitionKey);

    RuntimeState* _state;
    ObjectPool* _pool;
    const RowDescriptor& _row_desc;
    const std::vector<TExpr>& _t_output_expr;
    int _chunk_size{};
    const TIcebergTableSink* t_iceberg_sink;

    int _tuple_desc_id = -1;
    TupleDescriptor* _output_tuple_desc = nullptr;
    std::vector<ExprContext*> _output_expr_ctxs;

    RuntimeProfile* _profile = nullptr;
    int timeout_ms;
    std::unique_ptr<vectorized::Chunk> _output_chunk;
    std::vector<string> _partitionKeys;
    std::unordered_map<string, std::unique_ptr<FileBuilder>> _partition_writer_map;
};

} // namespace starrocks
