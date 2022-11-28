// This file_builder is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "orc_builder.h"

#include <utility>

#include "cctz/time_zone.h"
#include "column/chunk.h"
#include "column/const_column.h"
#include "column/datum_tuple.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "formats/csv/converter.h"
#include "gen_cpp/Descriptors_types.h"
#include "orc/Int128.hh"
#include "orc/MemoryPool.hh"
#include "orc/OrcFile.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "orc/orc-config.hh"
#include "runtime/buffer_control_block.h"
#include "runtime/decimalv2_value.h"
#include "runtime/decimalv3.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"
#include "util/date_func.h"
#include "util/logging.h"
#include "util/mysql_row_buffer.h"
#include "util/slice.h"
#include "util/timezone_utils.h"

namespace starrocks {

void fillStringValue(Column* dataColumn, orc::ColumnVectorBatch* batch, size_t row, size_t batchIdx);

template <typename IntegerType>
void fillIntegerValue(Column* dataColumn, orc::ColumnVectorBatch* batch, size_t row, size_t batchIdx);

template <typename FloatType>
void fillFloatValue(Column* dataColumn, orc::ColumnVectorBatch* batch, size_t row, size_t batchIdx);

void fillDecimalV2Value(Column* dataColumn, orc::ColumnVectorBatch* batch, size_t row, size_t batchIdx,
                        size_t precision, size_t scale);

template <typename DecimalType>
void fillDecimalV3Value(Column* dataColumn, orc::ColumnVectorBatch* batch, size_t row, size_t batchIdx,
                        size_t precision, size_t scale);

void fillDateValue(Column* dataColumn, orc::ColumnVectorBatch* batch, size_t row, size_t batchIdx);

void fillTimestampValue(Column* dataColumn, orc::ColumnVectorBatch* batch, size_t row, size_t batchIdx);

ORCOutputStream::ORCOutputStream(std::unique_ptr<WritableFile> writable_file, size_t buff_size) :
        _writable_file(std::move(writable_file)),
        _buff(new char[std::max(kMinBuffSize, buff_size)]),
        _pos(_buff),
        _end(_buff + std::max(kMinBuffSize, buff_size)) {
}
ORCOutputStream::~ORCOutputStream() {
    delete[] _buff;
}

uint64_t ORCOutputStream::getLength() const {
    return _writable_file->size();
}
uint64_t ORCOutputStream::getNaturalWriteSize() const {
    return _writable_file->size();
}
void ORCOutputStream::write(const void* buf, size_t length) {
    Slice value((const uint8_t *) buf, length);
    while (value.size > (_end - _pos)) {
        size_t ncopy = _end - _pos;
        memcpy(_pos, value.data, ncopy);
        _pos += ncopy;
        flush();
        value.remove_prefix(ncopy);
    }
    memcpy(_pos, value.data, value.size);
    _pos += value.size;
}

const std::string& ORCOutputStream::getName() const {
    return _writable_file->filename();
}
void ORCOutputStream::close() {
    if (_pos != _buff) {
        flush();
    }
    _writable_file->close();
}

void ORCOutputStream::flush() {
    Slice value((const uint8_t*)_buff, _pos - _buff);
    _writable_file->append(value);
    _pos = _buff;
}

const static std::unordered_map<LogicalType, orc::TypeKind> g_starrocks_orc_type_mapping = {
        {TYPE_BOOLEAN, orc::BOOLEAN},    {TYPE_TINYINT, orc::BYTE},
        {TYPE_SMALLINT, orc::SHORT},     {TYPE_INT, orc::INT},
        {TYPE_BIGINT, orc::LONG},        {TYPE_FLOAT, orc::FLOAT},
        {TYPE_DOUBLE, orc::DOUBLE},      {TYPE_DECIMALV2, orc::DECIMAL},
        {TYPE_DATE, orc::DATE},          {TYPE_DATETIME, orc::TIMESTAMP},
        {TYPE_CHAR, orc::CHAR},          {TYPE_VARCHAR, orc::VARCHAR},
        {TYPE_DECIMAL32, orc::DECIMAL},  {TYPE_DECIMAL64, orc::DECIMAL},
        {TYPE_DECIMAL128, orc::DECIMAL},
};

static Status _starrocks_type_to_orc_type_descriptor(const TypeDescriptor& starrocks_type,
                                                     std::unique_ptr<orc::Type>& result) {
    if (starrocks_type.type == TYPE_ARRAY) {
        return Status::NotSupported("Unsupported StarRocks type: " + starrocks_type.debug_string());
    }

    auto precision = (int)starrocks_type.precision;
    if (precision < 0) {
        precision = starrocks::TypeDescriptor::MAX_PRECISION;
    }
    auto scale = (int)starrocks_type.scale;
    if (scale < 0) {
        scale = starrocks::TypeDescriptor::MAX_SCALE;
    }
    auto len = (int)starrocks_type.len;
    auto iter = g_starrocks_orc_type_mapping.find(starrocks_type.type);
    if (iter == g_starrocks_orc_type_mapping.end()) {
        return Status::NotSupported("Unsupported ORC type: " + starrocks_type.debug_string());
    }
    auto type = iter->second;
    switch (starrocks_type.type) {
    case TYPE_CHAR: {
        if (len < 0) {
            len = starrocks::TypeDescriptor::MAX_CHAR_LENGTH;
        }
        result = orc::createCharType(orc::CHAR, len);
        break;
    }
    case TYPE_VARCHAR: {
        if (len < 0) {
            len = starrocks::TypeDescriptor::MAX_VARCHAR_LENGTH;
        }
        result = orc::createCharType(orc::VARCHAR, len);
        break;
    }
    case TYPE_DECIMAL128:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL32:
    case TYPE_DECIMALV2: {
        result = orc::createDecimalType(precision, scale);
        break;
    }
    default: {
        result = orc::createPrimitiveType(type);
    }
    }

    return Status::OK();
}

const size_t ORCBuilder::OUTSTREAM_BUFFER_SIZE_BYTES = 1024 * 1024;

ORCBuilder::ORCBuilder(ORCBuilderOptions options, std::unique_ptr<WritableFile> writable_file,
                       const std::vector<ExprContext*>& output_expr_ctxs,
                       TupleDescriptor* _output_tuple_desc,
                       std::vector<std::string>  column_names,
                       std::map<std::string, TExtraColumnAttribute> attributes)
        : _options(options),
          _output_expr_ctxs(output_expr_ctxs),
          _output_tuple_desc(_output_tuple_desc),
          _batchSize(1024),
          _init(false),
          _writable_file(std::move(writable_file)),
          _column_names(std::move(column_names)),
          _attributes(std::move(attributes)) {}

ORCBuilder::~ORCBuilder() {
}

Status ORCBuilder::init(Chunk* chunk) {
    if (_init || chunk->num_rows() == 0) {
        return Status::OK();
    }

    size_t num_fields = _output_expr_ctxs.size();
    std::unique_ptr<orc::Type> schema = orc::createStructType();

    for (int idx = 0; idx < num_fields; idx++) {
        // resolve column type
        const auto type = _output_expr_ctxs.at(idx)->root()->type();
        std::unique_ptr<orc::Type> fieldType;
        Status status = _starrocks_type_to_orc_type_descriptor(type, fieldType);
        if (!status.ok()) {
            return status;
        }

        // resolve column name
        auto root = _output_expr_ctxs.at(idx)->root();
        auto column_id = root->get_column_ref()->slot_id();
        std::string column_name;
        if (chunk->schema() != nullptr && !chunk->get_column_name(column_id).empty()) {
            column_name = chunk->get_column_name(column_id);
        } else if (_output_tuple_desc != nullptr) {
            column_name = _output_tuple_desc->slots()[idx]->col_name();
        } else if (_column_names.size() > idx) {
            column_name = _column_names[idx];
        } else {
            column_name = "col" + std::to_string(idx);
        }

        // fill column attribute
        auto it = this->_attributes.find(column_name);
        if (it != this->_attributes.end()) {
            auto value = (TExtraColumnAttribute) it->second;
            for (auto attribute = value.attributes.begin(); attribute != value.attributes.end(); attribute++) {
                fieldType->setAttribute(attribute->first, attribute->second);
            }
        }

        schema->addStructField(column_name, std::move(fieldType));
    }

    this->_type = std::move(schema);
    _outStream = std::unique_ptr<orc::OutputStream>(
            new ORCOutputStream(std::move(_writable_file), OUTSTREAM_BUFFER_SIZE_BYTES));

    _memory_pool = orc::getDefaultPool();
    orc::WriterOptions writerOptions;
    writerOptions.setMemoryPool(_memory_pool);
    writerOptions.setTimezoneName("UTC");
    if (_options.stripe_size > 0) {
        writerOptions.setStripeSize(_options.stripe_size);
    }
    if (_options.compression_block_size > 0) {
        writerOptions.setCompressionBlockSize(_options.compression_block_size);
    }
    writerOptions.setCompression(_options.compression_kind);
    writerOptions.setCompressionStrategy(_options.compression_strategy);

    _writer = orc::createWriter(*this->_type, _outStream.get(), writerOptions);

    _batch = _writer->createRowBatch(_batchSize);

    _init = true;

    return Status::OK();
}

Status ORCBuilder::add_chunk(Chunk* chunk) {
    RETURN_IF_ERROR(init(chunk));

    const size_t numRows = chunk->num_rows();
    size_t numFields = _output_expr_ctxs.size();
    size_t remainRows = numRows;
    size_t batchWriteRows;  // write rows in one loop
    size_t finishedRows = 0; // already finish write rows
    size_t planRows = 0;     // finished write rows when current loop end

    while (remainRows > 0) {
        if (remainRows > _batchSize) {
            batchWriteRows = _batchSize;
            remainRows -= _batchSize;
        } else {
            batchWriteRows = remainRows;
            remainRows = 0;
        }
        planRows += batchWriteRows;

        auto* structBatch = down_cast<orc::StructVectorBatch*>(_batch.get());
        structBatch->numElements = batchWriteRows;

        for (uint64_t col = 0; col < numFields; ++col) {
            const orc::Type* subType = this->_type->getSubtype(col);
            auto column = chunk->get_column_by_index(col);
            auto* curBatch = structBatch->fields[col];
            size_t precision = subType->getPrecision();
            size_t scale = subType->getScale();

            Column* dataColumn = column.get();
            NullColumn* nullColumn = nullptr;
            bool isNullable = false;
            if (_output_expr_ctxs.at(col)->root()->is_nullable()) {
                auto* nullableColumn = down_cast<NullableColumn*>(column.get());
                nullColumn = nullableColumn->null_column().get();
                dataColumn = nullableColumn->data_column().get();
                isNullable = true;
                curBatch->hasNulls = nullableColumn->has_null();
            }

            const auto& srType = _output_expr_ctxs.at(col)->root()->type();
            for (size_t row = finishedRows, batchIdx = 0; row < finishedRows + batchWriteRows; row++, batchIdx++) {
                // handle null row values
                if (isNullable && nullColumn->get_data()[row] != 0) {
                    curBatch->notNull[batchIdx] = 0;
                    continue;
                }

                curBatch->notNull[batchIdx] = 1;
                switch (srType.type) {
                    case TYPE_BOOLEAN:
                        fillIntegerValue<RunTimeCppType<TYPE_BOOLEAN>>(dataColumn, curBatch, row, batchIdx);
                        break;
                    case TYPE_TINYINT:
                        fillIntegerValue<RunTimeCppType<TYPE_TINYINT>>(dataColumn, curBatch, row, batchIdx);
                        break;
                    case TYPE_SMALLINT:
                        fillIntegerValue<RunTimeCppType<TYPE_SMALLINT>>(dataColumn, curBatch, row, batchIdx);
                        break;
                    case TYPE_INT:
                        fillIntegerValue<RunTimeCppType<TYPE_INT>>(dataColumn, curBatch, row, batchIdx);
                        break;
                    case TYPE_BIGINT:
                        fillIntegerValue<RunTimeCppType<TYPE_BIGINT>>(dataColumn, curBatch, row, batchIdx);
                        break;
                    case TYPE_FLOAT:
                        fillFloatValue<RunTimeCppType<TYPE_FLOAT>>(dataColumn, curBatch, row, batchIdx);
                        break;
                    case TYPE_DOUBLE:
                        fillFloatValue<RunTimeCppType<TYPE_DOUBLE>>(dataColumn, curBatch, row, batchIdx);
                        break;
                    case TYPE_CHAR:
                    case TYPE_VARCHAR:
                        fillStringValue(dataColumn, curBatch, row, batchIdx);
                        break;
                    case TYPE_DATE:
                        fillDateValue(dataColumn, curBatch, row, batchIdx);
                        break;
                    case TYPE_DATETIME:
                        fillTimestampValue(dataColumn, curBatch, row, batchIdx);
                        break;
                    case TYPE_DECIMALV2:
                        fillDecimalV2Value(dataColumn, curBatch, row, batchIdx, precision, scale);
                        break;
                    case TYPE_DECIMAL32:
                        fillDecimalV3Value<RunTimeCppType<TYPE_DECIMAL32>>(
                            dataColumn, curBatch, row, batchIdx, precision, scale);
                        break;
                    case TYPE_DECIMAL64:
                        fillDecimalV3Value<RunTimeCppType<TYPE_DECIMAL64>>(
                            dataColumn, curBatch, row, batchIdx, precision, scale);
                        break;
                    case TYPE_DECIMAL128:
                        fillDecimalV3Value<RunTimeCppType<TYPE_DECIMAL128>>(
                            dataColumn, curBatch, row, batchIdx, precision, scale);
                        break;
                    default:
                        return Status::NotSupported(std::to_string(srType.type) + " is not supported yet.");
                }
            }

            curBatch->numElements = batchWriteRows;
        }
        finishedRows = planRows;
        _writer->add(*_batch);
    }

    return Status::OK();
}

std::size_t ORCBuilder::file_size() {
    DCHECK(_writer != nullptr);
    return _outStream->getLength();
}

Status ORCBuilder::finish() {
    if (_writer == nullptr) {
        LOG(WARNING) << "ORCBuilder::_writer is nullptr";
        return Status::OK();
    }

    _writer->close();
    return Status::OK();
}


void fillStringValue(Column* dataColumn, orc::ColumnVectorBatch* batch, size_t row, size_t batchIdx) {
    auto* stringBatch = down_cast<orc::StringVectorBatch*>(batch);
    auto* binaryColumn = down_cast<BinaryColumn*>(dataColumn);
    auto& bytes = binaryColumn->get_bytes();
    const auto& offset = binaryColumn->get_offset();
    auto size = offset[row + 1] - offset[row];
    char* dataPtr = reinterpret_cast<char*>(&bytes[offset[row]]);
    stringBatch->data[batchIdx] = dataPtr;
    stringBatch->length[batchIdx] = size;
}

template <typename IntegerType>
void fillIntegerValue(Column* dataColumn, orc::ColumnVectorBatch* batch, size_t row, size_t batchIdx) {
    auto* longBatch = down_cast<orc::LongVectorBatch*>(batch);
    auto* numericColumn = down_cast<FixedLengthColumn<IntegerType>*>(dataColumn);
    longBatch->data[batchIdx] = numericColumn->get_data()[row];
}

template <typename FloatType>
void fillFloatValue(Column* dataColumn, orc::ColumnVectorBatch* batch, size_t row, size_t batchIdx) {
    auto* doubleBatch = down_cast<orc::DoubleVectorBatch*>(batch);
    auto* floatColumn = down_cast<FixedLengthColumn<FloatType>*>(dataColumn);
    doubleBatch->data[batchIdx] = floatColumn->get_data()[row];
}

static void fillDecimal64Batch(orc::ColumnVectorBatch* batch, int64_t decimal64, size_t row, size_t batchIdx,
                               size_t precision, size_t scale) {
    auto* d64Batch = down_cast<orc::Decimal64VectorBatch*>(batch);
    d64Batch->scale = static_cast<int32_t>(scale);
    d64Batch->values[batchIdx] = decimal64;
}

static void fillDecimal128Batch(orc::ColumnVectorBatch* batch, const std::string& decimalStrFmt, size_t row,
                                size_t batchIdx, size_t precision, size_t scale) {
    auto* d128Batch = down_cast<orc::Decimal128VectorBatch*>(batch);
    d128Batch->scale = static_cast<int32_t>(scale);
    size_t dotPos = decimalStrFmt.find('.');
    size_t curScale = 0;
    std::string int128Str;
    if (dotPos != std::string::npos) {
        curScale = decimalStrFmt.length() - dotPos - 1;
        int128Str = decimalStrFmt.substr(0, dotPos) + decimalStrFmt.substr(dotPos + 1);
    }
    orc::Int128 decimal(int128Str);
    while (curScale != scale) {
        curScale++;
        decimal *= 10;
    }

    d128Batch->values[batchIdx] = decimal;
}

void fillDecimalV2Value(Column *dataColumn, orc::ColumnVectorBatch *batch, size_t row, size_t batchIdx,
                        size_t precision, size_t scale) {
    auto* decimalColumn = down_cast<FixedLengthColumn<DecimalV2Value>*>(dataColumn);
    std::string decimalStrFmt = decimalColumn->get_data()[row].to_string(scale);
    fillDecimal128Batch(batch, decimalStrFmt, row, batchIdx, precision, scale);
}

template <typename DecimalType>
void fillDecimalV3Value(Column* dataColumn, orc::ColumnVectorBatch* batch, size_t row, size_t batchIdx,
                                    size_t precision, size_t scale) {
    auto* decimalColumn = down_cast<DecimalV3Column<DecimalType>*>(dataColumn);
    if constexpr (!std::is_same<DecimalType, RunTimeCppType<TYPE_DECIMAL128>>::value) {
        auto value = decimalColumn->get_data()[row];
        fillDecimal64Batch(batch, value, row, batchIdx, precision, scale);
    } else {
        auto decimalStrFmt = DecimalV3Cast::to_string<DecimalType>(decimalColumn->get_data()[row], precision, scale);
        fillDecimal128Batch(batch, decimalStrFmt, row, batchIdx, precision, scale);
    }
}

void fillDateValue(Column* dataColumn, orc::ColumnVectorBatch* batch, size_t row, size_t batchIdx) {
    auto* longBatch = down_cast<orc::LongVectorBatch*>(batch);
    const auto& t1970 = DateValue::create(1970, 1, 1);
    auto* dateColumn = down_cast<FixedLengthColumn<DateValue>*>(dataColumn);
    double days = dateColumn->get_data()[row].julian() - t1970.julian();
    longBatch->data[batchIdx] = static_cast<int64_t>(days);
}

void fillTimestampValue(Column* dataColumn, orc::ColumnVectorBatch* batch, size_t row, size_t batchIdx) {
    auto* tsBatch = down_cast<orc::TimestampVectorBatch*>(batch);
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, ctz);
    int64_t offset = TimezoneUtils::to_utc_offset(ctz);
    auto* dtColumn = down_cast<FixedLengthColumn<TimestampValue>*>(dataColumn);
    auto ts = dtColumn->get_data()[row].to_unix_second() - offset;
    tsBatch->data[batchIdx] = ts;
    tsBatch->nanoseconds[batchIdx] = 0;
}

} // namespace starrocks
