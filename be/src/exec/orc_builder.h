// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <utility>

#include "column/datum_tuple.h"
#include "exec/file_builder.h"
#include "formats/csv/converter.h"
#include "gen_cpp/Types_types.h"
#include "orc/MemoryPool.hh"
#include "orc/OrcFile.hh"
#include "orc/Vector.hh"

namespace starrocks {

class ExprContext;
class FileWriter;

struct ORCBuilderOptions {
    uint64_t stripe_size;
    uint64_t compression_block_size;
    orc::CompressionKind compression_kind;
    orc::CompressionStrategy compression_strategy;
};

class ORCOutputStream : public orc::OutputStream {
public:
    constexpr static const size_t kMinBuffSize = 128;
    ORCOutputStream(std::unique_ptr<WritableFile> writable_file, size_t buff_size);
    ~ORCOutputStream() override;

    uint64_t getLength() const override;
    uint64_t getNaturalWriteSize() const override;
    void write(const void* buf, size_t length) override;
    const std::string& getName() const override;
    void close() override;
    void flush();

private:
    std::unique_ptr<WritableFile> _writable_file;

    char* _buff;
    char* _pos;
    char* _end;
};

class ORCBuilder final : public FileBuilder {
public:
    ORCBuilder(ORCBuilderOptions options, std::unique_ptr<WritableFile> writable_file,
               const std::vector<ExprContext*>& output_expr_ctxs,
               TupleDescriptor* _output_tuple_desc,
               std::vector<std::string>  column_names,
               std::map<std::string, TExtraColumnAttribute> attributes);
    ~ORCBuilder();

    Status add_chunk(Chunk* chunk) override;

    std::size_t file_size() override;

    Status finish() override;

private:
    const static size_t OUTSTREAM_BUFFER_SIZE_BYTES;
    const ORCBuilderOptions _options;
    const std::vector<ExprContext*>& _output_expr_ctxs;
    ORC_UNIQUE_PTR<orc::Writer> _writer;
    std::unique_ptr<orc::Type> _type;
    std::unique_ptr<orc::ColumnVectorBatch> _batch;
    TupleDescriptor* _output_tuple_desc;
    std::unique_ptr<orc::OutputStream> _outStream;
    uint64_t _batchSize;
    bool _init;
    std::unique_ptr<WritableFile> _writable_file;
    orc::MemoryPool* _memory_pool;
    const std::vector<std::string> _column_names;
    std::map<std::string, TExtraColumnAttribute> _attributes;

    Status init(Chunk* chunk);
};

} // namespace starrocks
