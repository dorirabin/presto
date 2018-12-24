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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_MAX_HITS_EXCEEDED;
import static com.facebook.presto.elasticsearch.ElasticsearchUtils.serializeObject;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ElasticsearchRecordCursor
        implements RecordCursor
{
    private static final JsonCodec<Object> VALUE_CODEC = jsonCodec(Object.class);

    private final List<ElasticsearchColumnHandle> columnHandles;
    private final Map<String, Integer> jsonPathToIndex = new HashMap<>();
    private final int maxHits;
    private final Iterator<SearchHit> searchHits;
    private final Duration timeout;
    private final ElasticsearchQueryBuilder builder;

    private long totalBytes;
    private List<Object> fields;

    public ElasticsearchRecordCursor(List<ElasticsearchColumnHandle> columnHandles, ElasticsearchConnectorConfig config, ElasticsearchSplit split)
    {
        requireNonNull(columnHandles, "columnHandle is null");
        requireNonNull(config, "config is null");

        this.columnHandles = columnHandles;
        this.maxHits = config.getMaxHits();
        this.timeout = config.getRequestTimeout();

        for (int i = 0; i < columnHandles.size(); i++) {
            jsonPathToIndex.put(columnHandles.get(i).getColumnJsonPath(), i);
        }
        this.builder = new ElasticsearchQueryBuilder(columnHandles, config, split);
        this.searchHits = sendElasticQuery(builder).iterator();
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!searchHits.hasNext()) {
            return false;
        }

        SearchHit hit = searchHits.next();
        fields = new ArrayList<>(Collections.nCopies(columnHandles.size(), null));

        setFieldIfExists("_id", hit.getId());
        setFieldIfExists("_index", hit.getIndex());

        extractFromSource(hit);
        if (hit.getSourceRef() != null) {
            totalBytes += hit.getSourceRef().length();
        }
        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, ImmutableSet.of(BOOLEAN));
        return (Boolean) getFieldValue(field);
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, ImmutableSet.of(BIGINT, INTEGER));
        return (Integer) getFieldValue(field);
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, ImmutableSet.of(DOUBLE));
        return (Double) getFieldValue(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, ImmutableSet.of(VARCHAR));

        Object value = getFieldValue(field);
        if (value instanceof Collection) {
            return utf8Slice(VALUE_CODEC.toJson(value));
        }
        if (value == null) {
            return EMPTY_SLICE;
        }
        return utf8Slice(String.valueOf(value));
    }

    @Override
    public Object getObject(int field)
    {
        return serializeObject(columnHandles.get(field).getColumnType(), null, getFieldValue(field));
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return getFieldValue(field) == null;
    }

    private void checkFieldType(int field, Set<Type> expectedTypes)
    {
        checkArgument(expectedTypes.contains(getType(field)), "Field %s has unexpected type %s", field, getType(field));
    }

    @Override
    public void close()
    {
        builder.close();
    }

    private List<SearchHit> sendElasticQuery(ElasticsearchQueryBuilder queryBuilder)
    {
        ImmutableList.Builder<SearchHit> result = ImmutableList.builder();
        SearchResponse response = queryBuilder.buildScrollSearchRequest()
                .execute()
                .actionGet(timeout.toMillis());

        if (response.getHits().getTotalHits() > maxHits) {
            throw new PrestoException(ELASTICSEARCH_MAX_HITS_EXCEEDED,
                    format("The number of hits for the query (%d) exceeds the configured max hits (%d)", response.getHits().getTotalHits(), maxHits));
        }

        while (true) {
            for (SearchHit hit : response.getHits().getHits()) {
                result.add(hit);
            }
            response = queryBuilder.prepareSearchScroll(response.getScrollId())
                    .execute()
                    .actionGet(timeout.toMillis());
            if (response.getHits().getHits().length == 0) {
                break;
            }
        }
        return result.build();
    }

    private void setFieldIfExists(String jsonPath, Object jsonValue)
    {
        if (jsonPathToIndex.containsKey(jsonPath)) {
            fields.set(jsonPathToIndex.get(jsonPath), jsonValue);
        }
    }

    private Object getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");
        return fields.get(field);
    }

    private void extractFromSource(SearchHit hit)
    {
        Map<String, Object> originalMap = hit.getSourceAsMap();
        String separator = Pattern.quote(".");
        //loop on all the fields and try to extract their value
        for (String fieldName : jsonPathToIndex.keySet()) {
            //fieldName can have jsonpath nested structure like this <partA>.<partsB>.<partC>
            String[] fieldNameParts = fieldName.split(separator);
            Map<String, Object> map = originalMap;
            //if we find match between structure and part A then we can go to the next part B and dig deeper in the structure
            // until we reach either the last field name part or the end of the structure
            for (int i = 0; i < fieldNameParts.length && map != null && map.containsKey(fieldNameParts[i]); i++) {
                //extract the value
                Object value = map.get(fieldNameParts[i]);
                //if we reached the final part of the fieldName and we have a full match then we can assign and stop
                if (i == fieldNameParts.length - 1) {
                    setFieldIfExists(fieldName, value);
                }
                else { //let's check the value type if it's a map we can go deeper
                    if (value instanceof Map<?, ?>) {
                        //we have a new map to explore
                        map = (Map<String, Object>) value;
                    }
                    else {
                        map = null;
                    }
                }
            }
        }
    }
}
