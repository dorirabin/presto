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

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.file.Files.readAllBytes;
import static java.util.Objects.requireNonNull;

public class ElasticsearchTableDescriptionProvider
{
    private final Map<SchemaTableName, ElasticsearchTableDescription> tableDefinitions;

    @Inject
    ElasticsearchTableDescriptionProvider(ElasticsearchConnectorConfig config, JsonCodec<ElasticsearchTableDescription> codec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(codec, "codec is null");
        checkArgument(config.getScrollTimeout().toMillis() >= 10, "scrollTimeout must be at least 10 ms");
        checkArgument(config.getRequestTimeout().toMillis() >= 10, "requestTimeout must be at least 10 ms");
        tableDefinitions = createTableDescriptions(config, codec);
    }

    private Map<SchemaTableName, ElasticsearchTableDescription> createTableDescriptions(ElasticsearchConnectorConfig config, JsonCodec<ElasticsearchTableDescription> codec)
    {
        ImmutableMap.Builder<SchemaTableName, ElasticsearchTableDescription> builder = ImmutableMap.builder();

        try {
            for (File file : listFiles(config.getTableDescriptionDirectory())) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    ElasticsearchTableDescription table = codec.fromJson(readAllBytes(file.toPath()));
                    String schemaName = firstNonNull(table.getSchemaName(), config.getDefaultSchema());
                    builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
            }
            return builder.build();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<File> listFiles(File directory)
    {
        if (directory != null && directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    public ElasticsearchTableDescription get(SchemaTableName schemaTableName)
    {
        return tableDefinitions.get(schemaTableName);
    }

    public Set<SchemaTableName> getAllSchemaTableNames()
    {
        return tableDefinitions.keySet();
    }

    public Set<ElasticsearchTableDescription> getAllTableDescriptions()
    {
        return ImmutableSet.copyOf(tableDefinitions.values());
    }
}
