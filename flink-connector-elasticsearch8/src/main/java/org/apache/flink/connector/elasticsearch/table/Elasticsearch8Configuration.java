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

import org.apache.flink.configuration.ReadableConfig;

import java.util.Optional;

import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.ALLOW_INSECURE;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.MAX_RETRIES;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.NUM_CANDIDATES;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_CERT_FINGERPRINT;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_HTTP_CA;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_KEYSTORE;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_KEYSTORE_PASSWORD;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_KEYSTORE_TYPE;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_KEY_PASSWORD;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_PROTOCOL;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_TRUSTSTORE;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.SSL_TRUSTSTORE_TYPE;

/** Elasticsearch 8 specific configuration. */
public class Elasticsearch8Configuration extends ElasticsearchConfiguration {
    Elasticsearch8Configuration(ReadableConfig config) {
        super(config);
    }

    public int getMaxRetries() {
        return config.get(MAX_RETRIES);
    }

    public int getNumCandidates() {
        return config.get(NUM_CANDIDATES);
    }

    public Optional<String> getSslProtocol() {
        return config.getOptional(SSL_PROTOCOL);
    }

    public Optional<Boolean> getAllowInsecure() {
        return config.getOptional(ALLOW_INSECURE);
    }

    public Optional<String> getSslKeystore() {
        return config.getOptional(SSL_KEYSTORE);
    }

    public Optional<String> getSslKeyStoreType() {
        return config.getOptional(SSL_KEYSTORE_TYPE);
    }

    public Optional<String> getSslKeystorePassword() {
        return config.getOptional(SSL_KEYSTORE_PASSWORD);
    }

    public Optional<String> getSslKeyPassword() {
        return config.getOptional(SSL_KEY_PASSWORD);
    }

    public Optional<String> getSslTruststore() {
        return config.getOptional(SSL_TRUSTSTORE);
    }

    public Optional<String> getSslTruststoreType() {
        return config.getOptional(SSL_TRUSTSTORE_TYPE);
    }

    public Optional<String> getSslTruststorePassword() {
        return config.getOptional(SSL_TRUSTSTORE_PASSWORD);
    }

    public Optional<String> getSslCertFingerprint() {
        return config.getOptional(SSL_CERT_FINGERPRINT);
    }

    public Optional<String> getSslHttpCa() {
        return config.getOptional(SSL_HTTP_CA);
    }
}
