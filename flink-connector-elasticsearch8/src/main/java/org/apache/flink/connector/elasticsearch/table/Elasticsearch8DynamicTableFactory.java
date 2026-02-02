/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.StringUtils;

import java.time.ZoneId;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.ALLOW_INSECURE;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.MAX_RETRIES;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_CERT_FINGERPRINT;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_HTTP_CA;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_KEY_PASSWORD;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_KEYSTORE;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_KEYSTORE_PASSWORD;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_KEYSTORE_TYPE;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_PROTOCOL;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_TRUSTSTORE;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_TRUSTSTORE_TYPE;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_INTERVAL_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_MAX_SIZE_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.CONNECTION_PATH_PREFIX_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.CONNECTION_REQUEST_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.CONNECTION_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.DELIVERY_GUARANTEE_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.FORMAT_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.HOSTS_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.INDEX_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.KEY_DELIMITER_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.PASSWORD_OPTION;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.SOCKET_TIMEOUT;
import static org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions.USERNAME_OPTION;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.CACHE_TYPE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_MAX_ROWS;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

/** A {@link DynamicTableSinkFactory} for discovering {@link ElasticsearchDynamicSink}. */
@Internal
public class Elasticsearch8DynamicTableFactory implements DynamicTableSourceFactory {
    private static final String FACTORY_IDENTIFIER = "elasticsearch-8";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        final DecodingFormat<DeserializationSchema<RowData>> format =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class,
                        org.apache.flink.connector.elasticsearch.table.ElasticsearchConnectorOptions
                                .FORMAT_OPTION);

        Elasticsearch8Configuration config = getConfiguration(helper);
        helper.validate();
        validateConfiguration(config);

        return new Elasticsearch8DynamicSource(
                format,
                config,
                context.getPhysicalRowDataType(),
                "Elasticsearch-8");
    }

    Elasticsearch8Configuration getConfiguration(FactoryUtil.TableFactoryHelper helper) {
        return new Elasticsearch8Configuration(helper.getOptions());
    }

    void validateConfiguration(ElasticsearchConfiguration config) {
        config.getHosts(); // validate hosts
        validate(
                config.getIndex().length() >= 1,
                () -> String.format("'%s' must not be empty", INDEX_OPTION.key()));
        int maxActions = config.getBulkFlushMaxActions();
        validate(
                maxActions == -1 || maxActions >= 1,
                () ->
                        String.format(
                                "'%s' must be at least 1. Got: %s",
                                BULK_FLUSH_MAX_ACTIONS_OPTION.key(), maxActions));
        long maxSize = config.getBulkFlushMaxByteSize().getBytes();
        long mb1 = 1024 * 1024;
        validate(
                maxSize == -1 || (maxSize >= mb1 && maxSize % mb1 == 0),
                () ->
                        String.format(
                                "'%s' must be in MB granularity. Got: %s",
                                BULK_FLUSH_MAX_SIZE_OPTION.key(),
                                config.getBulkFlushMaxByteSize().toHumanReadableString()));
        validate(
                config.getBulkFlushBackoffRetries().map(retries -> retries >= 1).orElse(true),
                () ->
                        String.format(
                                "'%s' must be at least 1. Got: %s",
                                BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION.key(),
                                config.getBulkFlushBackoffRetries().get()));
        if (config.getUsername().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())) {
            validate(
                    config.getPassword().isPresent()
                            && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get()),
                    () ->
                            String.format(
                                    "'%s' and '%s' must be set at the same time. Got: username '%s' and password '%s'",
                                    USERNAME_OPTION.key(),
                                    PASSWORD_OPTION.key(),
                                    config.getUsername().get(),
                                    config.getPassword().orElse("")));
        }
    }

    static void validate(boolean condition, Supplier<String> message) {
        if (!condition) {
            throw new ValidationException(message.get());
        }
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Stream.of(HOSTS_OPTION, INDEX_OPTION).collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Stream.of(
                        KEY_DELIMITER_OPTION,
                        BULK_FLUSH_MAX_SIZE_OPTION,
                        BULK_FLUSH_MAX_ACTIONS_OPTION,
                        BULK_FLUSH_INTERVAL_OPTION,
                        BULK_FLUSH_BACKOFF_TYPE_OPTION,
                        BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION,
                        BULK_FLUSH_BACKOFF_DELAY_OPTION,
                        CONNECTION_PATH_PREFIX_OPTION,
                        CONNECTION_REQUEST_TIMEOUT,
                        CONNECTION_TIMEOUT,
                        SOCKET_TIMEOUT,
                        FORMAT_OPTION,
                        DELIVERY_GUARANTEE_OPTION,
                        PASSWORD_OPTION,
                        USERNAME_OPTION,
                        SINK_PARALLELISM,
                        CACHE_TYPE,
                        PARTIAL_CACHE_EXPIRE_AFTER_ACCESS,
                        PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
                        PARTIAL_CACHE_MAX_ROWS,
                        PARTIAL_CACHE_CACHE_MISSING_KEY,
                        MAX_RETRIES,
                        SSL_KEYSTORE,
                        SSL_KEYSTORE_TYPE,
                        SSL_KEYSTORE_PASSWORD,
                        SSL_KEY_PASSWORD,
                        SSL_TRUSTSTORE,
                        SSL_TRUSTSTORE_TYPE,
                        SSL_TRUSTSTORE_PASSWORD,
                        SSL_CERT_FINGERPRINT,
                        SSL_PROTOCOL,
                        SSL_HTTP_CA,
                        ALLOW_INSECURE)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(
                        HOSTS_OPTION,
                        INDEX_OPTION,
                        PASSWORD_OPTION,
                        USERNAME_OPTION,
                        KEY_DELIMITER_OPTION,
                        BULK_FLUSH_MAX_ACTIONS_OPTION,
                        BULK_FLUSH_MAX_SIZE_OPTION,
                        BULK_FLUSH_INTERVAL_OPTION,
                        BULK_FLUSH_BACKOFF_TYPE_OPTION,
                        BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION,
                        BULK_FLUSH_BACKOFF_DELAY_OPTION,
                        CONNECTION_PATH_PREFIX_OPTION,
                        CONNECTION_REQUEST_TIMEOUT,
                        CONNECTION_TIMEOUT,
                        SOCKET_TIMEOUT)
                .collect(Collectors.toSet());
    }

    @Override
    public String factoryIdentifier() {
        return FACTORY_IDENTIFIER;
    }
}
