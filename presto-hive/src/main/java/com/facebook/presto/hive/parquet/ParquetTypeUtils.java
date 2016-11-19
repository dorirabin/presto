/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.parquet;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.NestedField;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.io.ColumnIO;
import parquet.io.ColumnIOFactory;
import parquet.io.InvalidRecordException;
import parquet.io.ParquetDecodingException;
import parquet.io.PrimitiveColumnIO;
import parquet.schema.DecimalMetadata;
import parquet.schema.GroupType;
import parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static parquet.schema.OriginalType.DECIMAL;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

public final class ParquetTypeUtils
{
    private ParquetTypeUtils()
    {
    }

    public static List<PrimitiveColumnIO> getColumns(MessageType fileSchema, MessageType requestedSchema)
    {
        return (new ColumnIOFactory()).getColumnIO(requestedSchema, fileSchema, true).getLeaves();
    }

    public static Optional<RichColumnDescriptor> getDescriptor(MessageType fileSchema, MessageType requestedSchema, List<String> path)
    {
        checkArgument(path.size() >= 1, "Parquet nested path should have at least one component");
        int index = getPathIndex(fileSchema, requestedSchema, path);
        return getDescriptor(fileSchema, requestedSchema, index);
    }

    public static Optional<RichColumnDescriptor> getDescriptor(MessageType fileSchema, MessageType requestedSchema, int index)
    {
        if (index == -1) {
            return empty();
        }
        PrimitiveColumnIO columnIO = getColumns(fileSchema, requestedSchema).get(index);
        ColumnDescriptor descriptor = columnIO.getColumnDescriptor();
        return Optional.of(new RichColumnDescriptor(descriptor.getPath(), columnIO.getType().asPrimitiveType(), descriptor.getMaxRepetitionLevel(), descriptor.getMaxDefinitionLevel()));
    }

    private static int getPathIndex(MessageType fileSchema, MessageType requestedSchema, List<String> path)
    {
        int maxLevel = path.size();
        List<PrimitiveColumnIO> columns = getColumns(fileSchema, requestedSchema);
        int index = -1;
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            ColumnIO[] fields = columns.get(columnIndex).getPath();
            if (fields.length <= maxLevel) {
                continue;
            }
            if (fields[maxLevel].getName().equalsIgnoreCase(path.get(maxLevel - 1))) {
                boolean match = true;
                for (int level = 0; level < maxLevel - 1; level++) {
                    if (!fields[level + 1].getName().equalsIgnoreCase(path.get(level))) {
                        match = false;
                    }
                }

                if (match) {
                    index = columnIndex;
                }
            }
        }
        return index;
    }

    public static Type getPrestoType(RichColumnDescriptor descriptor)
    {
        switch (descriptor.getType()) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case BINARY:
                return createDecimalType(descriptor).orElse(VarcharType.VARCHAR);
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case INT32:
                return createDecimalType(descriptor).orElse(IntegerType.INTEGER);
            case INT64:
                return createDecimalType(descriptor).orElse(BigintType.BIGINT);
            case INT96:
                return TimestampType.TIMESTAMP;
            case FIXED_LEN_BYTE_ARRAY:
                return createDecimalType(descriptor).orElseThrow(() -> new PrestoException(NOT_SUPPORTED, "Parquet type FIXED_LEN_BYTE_ARRAY supported as DECIMAL; got " + descriptor.getPrimitiveType().getOriginalType()));
            default:
                throw new PrestoException(NOT_SUPPORTED, "Unsupported parquet type: " + descriptor.getType());
        }
    }

    public static int getFieldIndex(MessageType fileSchema, String name)
    {
        try {
            return fileSchema.getFieldIndex(name);
        }
        catch (InvalidRecordException e) {
            for (parquet.schema.Type type : fileSchema.getFields()) {
                if (type.getName().equalsIgnoreCase(name)) {
                    return fileSchema.getFieldIndex(type.getName());
                }
            }
            return -1;
        }
    }

    public static parquet.schema.Type getParquetType(HiveColumnHandle column, MessageType messageType, boolean useParquetColumnNames)
    {
        if (useParquetColumnNames) {
            return getParquetTypeByName(column.getName(), messageType);
        }

        if (column.getHiveColumnIndex() < messageType.getFieldCount()) {
            return messageType.getType(column.getHiveColumnIndex());
        }
        return null;
    }

    public static ParquetEncoding getParquetEncoding(Encoding encoding)
    {
        switch (encoding) {
            case PLAIN:
                return ParquetEncoding.PLAIN;
            case RLE:
                return ParquetEncoding.RLE;
            case BIT_PACKED:
                return ParquetEncoding.BIT_PACKED;
            case PLAIN_DICTIONARY:
                return ParquetEncoding.PLAIN_DICTIONARY;
            case DELTA_BINARY_PACKED:
                return ParquetEncoding.DELTA_BINARY_PACKED;
            case DELTA_LENGTH_BYTE_ARRAY:
                return ParquetEncoding.DELTA_LENGTH_BYTE_ARRAY;
            case DELTA_BYTE_ARRAY:
                return ParquetEncoding.DELTA_BYTE_ARRAY;
            case RLE_DICTIONARY:
                return ParquetEncoding.RLE_DICTIONARY;
            default:
                throw new ParquetDecodingException("Unsupported Parquet encoding: " + encoding);
        }
    }

    private static parquet.schema.Type getParquetTypeByName(String columnName, GroupType groupType)
    {
        if (groupType.containsField(columnName)) {
            return groupType.getType(columnName);
        }
        // parquet is case-sensitive, but hive is not. all hive columns get converted to lowercase
        // check for direct match above but if no match found, try case-insensitive match
        for (parquet.schema.Type type : groupType.getFields()) {
            if (type.getName().equalsIgnoreCase(columnName)) {
                return type;
            }
        }
        return null;
    }

    public static Optional<Type> createDecimalType(RichColumnDescriptor descriptor)
    {
        if (descriptor.getPrimitiveType().getOriginalType() != DECIMAL) {
            return Optional.empty();
        }
        DecimalMetadata decimalMetadata = descriptor.getPrimitiveType().getDecimalMetadata();
        return Optional.of(DecimalType.createDecimalType(decimalMetadata.getPrecision(), decimalMetadata.getScale()));
    }

    public static parquet.schema.Type getColumnType(HiveColumnHandle column, MessageType messageType, boolean useParquetColumnNames, Optional<Map<String, NestedField>> nestedFields)
    {
        parquet.schema.Type columnType = getParquetType(column, messageType, useParquetColumnNames);
        if (nestedFields.isPresent()) {
            NestedField nestedField = nestedFields.get().get(column.getName());
            if (nestedField != null) {
                return getFieldType(messageType, nestedField);
            }
        }
        return columnType;
    }

    private static parquet.schema.Type getFieldType(GroupType groupType, NestedField field)
    {
        parquet.schema.Type fieldType = getParquetTypeByName(field.getName(), groupType);
        if (fieldType == null) {
            return null;
        }

        Map<String, NestedField> children = field.getFields();
        if (children.isEmpty()) {
            return fieldType;
        }

        GroupType fields = (GroupType) fieldType;
        List<parquet.schema.Type> childrenTypes = new ArrayList<>(children.size());
        for (NestedField child : children.values()) {
            parquet.schema.Type parquetType = getFieldType(fields, child);
            if (parquetType != null) {
                childrenTypes.add(parquetType);
            }
        }

        if (childrenTypes.isEmpty()) {
            return fieldType;
        }

        Collections.sort(childrenTypes, new ParquetFieldComparator(fields));
        return new MessageType(fieldType.getName(), childrenTypes);
    }

    private static class ParquetFieldComparator
            implements Comparator<parquet.schema.Type>
    {
        private final GroupType fields;

        public ParquetFieldComparator(GroupType fields)
        {
            this.fields = requireNonNull(fields, "fields is null");
        }

        @Override
        public int compare(parquet.schema.Type left, parquet.schema.Type right)
        {
            return fields.getFieldIndex(left.getName()) - fields.getFieldIndex(right.getName());
        }
    }
}
