/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.connector.elasticsearch.sink.NetworkConfig;
import org.apache.flink.connector.elasticsearch.table.search.ElasticsearchRowDataVectorSearchFunction;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.VectorSearchTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.search.VectorSearchFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.SerializableSupplier;
import co.elastic.clients.transport.TransportUtils;
import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@link DynamicTableSource} that describes how to create a {@link Elasticsearch8DynamicSource}
 * from a logical description.
 */
public class Elasticsearch8DynamicSource
        implements VectorSearchTableSource, SupportsProjectionPushDown {

    protected final DecodingFormat<DeserializationSchema<RowData>> format;
    protected final Elasticsearch8Configuration config;
    private final String summaryString;
    protected DataType physicalRowDataType;

    public Elasticsearch8DynamicSource(
            DecodingFormat<DeserializationSchema<RowData>> format,
            Elasticsearch8Configuration config,
            DataType physicalRowDataType,
            String summaryString) {
        this.format = format;
        this.config = config;
        this.physicalRowDataType = physicalRowDataType;
        this.summaryString = summaryString;
    }

    @SuppressWarnings("unchecked")
    @Override
    public VectorSearchRuntimeProvider getSearchRuntimeProvider(
            VectorSearchContext vectorSearchContext) {

        NetworkConfig networkConfig;
        try {
            networkConfig = buildNetworkConfig();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        ElasticsearchRowDataVectorSearchFunction vectorSearchFunction =
                new ElasticsearchRowDataVectorSearchFunction(
                        this.format.createRuntimeDecoder(vectorSearchContext, physicalRowDataType),
                        config.getMaxRetries(),
                        config.getNumCandidates(),
                        config.getIndex(),
                        getSearchColumn(vectorSearchContext),
                        DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                        networkConfig);

        return VectorSearchFunctionProvider.of(vectorSearchFunction);
    }

    private String getSearchColumn(VectorSearchContext vectorSearchContext) {
        int[][] searchColumns = vectorSearchContext.getSearchColumns();

        if (searchColumns.length != 1) {
            throw new IllegalArgumentException(
                    String.format(
                            "Elasticsearch only supports one search columns now, but input search columns size is %d.",
                            searchColumns.length));
        }
        int[] searchColumn = searchColumns[0];
        if (searchColumn.length != 1) {
            throw new IllegalArgumentException(
                    "Elasticsearch doesn't support to search data using nested columns.");
        }
        int searchColumnIndex = searchColumn[0];

        if (searchColumnIndex < 0
                || searchColumnIndex >= physicalRowDataType.getChildren().size()) {
            throw new ValidationException(
                    String.format(
                            "The specified search column with index %d doesn't exist in schema.",
                            searchColumnIndex));
        }

        DataType searchColumnType = physicalRowDataType.getChildren().get(searchColumnIndex);
        if (!searchColumnType.getLogicalType().is(LogicalTypeRoot.ARRAY)
                || !((ArrayType) searchColumnType.getLogicalType())
                        .getElementType()
                        .is(LogicalTypeRoot.FLOAT)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Elasticsearch only supports search data using float vector now, but input search column type is %s.",
                            searchColumnType));
        }

        return ((RowType) (physicalRowDataType.getLogicalType()))
                .getFieldNames()
                .get(searchColumnIndex);
    }

    private NetworkConfig buildNetworkConfig() {
        List<HttpHost> hosts = config.getHosts();
        checkArgument(!hosts.isEmpty(), "Hosts cannot be empty.");

        String username = null;
        if (config.getUsername().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())) {
            username = config.getUsername().get();
        }

        String password = null;
        if (config.getPassword().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get())) {
            password = config.getPassword().get();
        }

        // Build SSL context supplier and hostname verifier
        SerializableSupplier<SSLContext> sslContextSupplier = buildSslContextSupplier();

        return new NetworkConfig(hosts, username, password, null, sslContextSupplier, null);
    }

    private SerializableSupplier<SSLContext> buildSslContextSupplier() {
        // Get SSL protocol, default is TLSv1.2
        String protocol =
                config.getSslProtocol()
                        .orElse(Elasticsearch8ConnectorOptions.SSL_PROTOCOL.defaultValue());

        // Check if keystore or truststore is configured
        final boolean hasKeystore = config.getSslKeystore().isPresent();
        final boolean hasTruststore = config.getSslTruststore().isPresent();
        final boolean hasHttpCa = config.getSslHttpCa().isPresent();

        if (config.getAllowInsecure()
                .orElse(Elasticsearch8ConnectorOptions.ALLOW_INSECURE.defaultValue())) {
            return () -> {
                try {
                    return SSLContexts.custom()
                            .loadTrustMaterial(TrustAllStrategy.INSTANCE)
                            .build();
                } catch (final NoSuchAlgorithmException
                        | KeyStoreException
                        | KeyManagementException ex) {
                    throw new IllegalStateException("Unable to create custom SSL context", ex);
                }
            };
        } else if (config.getSslCertFingerprint().isPresent()) {
            String certFingerprint = config.getSslCertFingerprint().get();
            return () -> TransportUtils.sslContextFromCaFingerprint(certFingerprint);
        } else if (hasKeystore || hasTruststore || hasHttpCa) {
            // Extract all config values as final variables for serializable lambda
            final String sslProtocol = protocol;

            // Keystore config
            final String keystorePath = hasKeystore ? config.getSslKeystore().get() : null;
            final String keystoreType =
                    hasKeystore
                            ? config.getSslKeyStoreType()
                                    .orElse(
                                            Elasticsearch8ConnectorOptions.SSL_KEYSTORE_TYPE
                                                    .defaultValue())
                            : null;
            final String keystorePassword =
                    hasKeystore
                            ? config.getSslKeystorePassword()
                                    .orElseThrow(
                                            () ->
                                                    new IllegalConfigurationException(
                                                            "The config option "
                                                                    + Elasticsearch8ConnectorOptions
                                                                            .SSL_KEYSTORE_PASSWORD
                                                                    + " is missing."))
                            : null;
            final String keyPassword =
                    hasKeystore
                            ? config.getSslKeyPassword()
                                    .orElseThrow(
                                            () ->
                                                    new IllegalConfigurationException(
                                                            "The config option "
                                                                    + Elasticsearch8ConnectorOptions
                                                                            .SSL_KEY_PASSWORD
                                                                    + " is missing."))
                            : null;

            // Truststore config
            final String truststorePath = hasTruststore ? config.getSslTruststore().get() : null;
            final String truststoreType =
                    (hasTruststore || hasHttpCa)
                            ? config.getSslTruststoreType()
                                    .orElse(
                                            Elasticsearch8ConnectorOptions.SSL_TRUSTSTORE_TYPE
                                                    .defaultValue())
                            : null;
            final String truststorePassword =
                    hasTruststore
                            ? config.getSslTruststorePassword()
                                    .orElseThrow(
                                            () ->
                                                    new IllegalConfigurationException(
                                                            "The config option "
                                                                    + Elasticsearch8ConnectorOptions
                                                                            .SSL_TRUSTSTORE_PASSWORD
                                                                    + " is missing."))
                            : null;
            final String httpCa = config.getSslHttpCa().orElse(null);

            return () -> {
                try {
                    KeyManager[] kms = null;
                    if (hasKeystore) {
                        KeyStore keystore = KeyStore.getInstance(keystoreType);
                        try (FileInputStream keystoreStream = new FileInputStream(keystorePath)) {
                            keystore.load(keystoreStream, keystorePassword.toCharArray());
                        }
                        KeyManagerFactory kmf =
                                KeyManagerFactory.getInstance(
                                        KeyManagerFactory.getDefaultAlgorithm());
                        kmf.init(keystore, keyPassword.toCharArray());
                        kms = kmf.getKeyManagers();
                    }

                    TrustManager[] tms = null;
                    if (hasTruststore || hasHttpCa) {
                        KeyStore trustStore = KeyStore.getInstance(truststoreType);
                        if (hasTruststore) {
                            try (InputStream trustStoreFile =
                                    Files.newInputStream(new File(truststorePath).toPath())) {
                                trustStore.load(trustStoreFile, truststorePassword.toCharArray());
                            }
                        } else {
                            trustStore.load(null, null);
                        }

                        TrustManagerFactory tmf =
                                TrustManagerFactory.getInstance(
                                        TrustManagerFactory.getDefaultAlgorithm());
                        if (!StringUtils.isNullOrWhitespaceOnly(httpCa)) {
                            CertificateFactory factory = CertificateFactory.getInstance("X.509");
                            Certificate ca =
                                    factory.generateCertificate(
                                            new ByteArrayInputStream(httpCa.getBytes()));
                            trustStore.setCertificateEntry("ca", ca);
                        }
                        tmf.init(trustStore);
                        tms = tmf.getTrustManagers();
                    }

                    SSLContext sslContext = SSLContext.getInstance(sslProtocol);
                    sslContext.init(kms, tms, null);
                    return sslContext;
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to create SSL context", e);
                }
            };
        }

        // No SSL configuration, return null
        return null;
    }

    @Override
    public DynamicTableSource copy() {
        return new Elasticsearch8DynamicSource(
                format, config, physicalRowDataType, summaryString);
    }

    @Override
    public String asSummaryString() {
        return summaryString;
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType type) {
        this.physicalRowDataType = Projection.of(projectedFields).project(type);
    }
}
