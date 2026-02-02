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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.security.KeyStore;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Options specific for the Elasticsearch 8 connector. Public so that the {@link
 * org.apache.flink.table.api.TableDescriptor} can access it.
 */
public class Elasticsearch8ConnectorOptions extends ElasticsearchConnectorOptions {
    private Elasticsearch8ConnectorOptions() {}

    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("max-retries")
                    .intType()
                    .defaultValue(3)
                    .withFallbackKeys("lookup.max-retries")
                    .withDescription(
                            "The maximum allowed retries if a lookup/search operation fails.");

    public static final ConfigOption<Integer> NUM_CANDIDATES =
            ConfigOptions.key("vector-search.num-candidates")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "The candidates when search.");

    public static final ConfigOption<String> SSL_KEYSTORE =
            key("security.ssl.keystore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Java keystore file to be used by the es endpoint for its SSL Key and Certificate.");

    public static final ConfigOption<String> SSL_KEYSTORE_TYPE =
            key("security.ssl.keystore-type")
                    .stringType()
                    .defaultValue(KeyStore.getDefaultType())
                    .withDescription(
                            "The type of keystore "
                                    + "for es endpoints (rpc, data transport, blob server).");

    public static final ConfigOption<String> SSL_KEYSTORE_PASSWORD =
            key("security.ssl.keystore-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The secret to decrypt the keystore file.");

    public static final ConfigOption<String> SSL_KEY_PASSWORD =
            key("security.ssl.key-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The secret to decrypt the server key in the keystore.");

    public static final ConfigOption<String> SSL_TRUSTSTORE =
            key("security.ssl.truststore")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The truststore file containing the public CA certificates to be used by es endpoints"
                                    + " to verify the peer’s certificate.");

    public static final ConfigOption<String> SSL_TRUSTSTORE_TYPE =
            key("security.ssl.truststore-type")
                    .stringType()
                    .defaultValue(KeyStore.getDefaultType())
                    .withDescription(
                            "The type of truststore "
                                    + "for es endpoints (rpc, data transport, blob server).");

    public static final ConfigOption<String> SSL_TRUSTSTORE_PASSWORD =
            key("security.ssl.truststore-password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The secret to decrypt the truststore.");

    public static final ConfigOption<String> SSL_CERT_FINGERPRINT =
            key("security.ssl.cert.fingerprint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The sha1 fingerprint of the certificate. "
                                    + "This further protects the communication to present the exact certificate used by es."
                                    + "This is necessary where one cannot use private CA(self signed) or there is internal firm wide CA is required");

    public static final ConfigOption<String> SSL_HTTP_CA =
            key("security.ssl.http-ca")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The sha1 fingerprint of the certificate. "
                                    + "This further protects the communication to present the exact certificate used by es."
                                    + "This is necessary where one cannot use private CA(self signed) or there is internal firm wide CA is required");

    public static final ConfigOption<String> SSL_PROTOCOL =
            key("security.ssl.protocol")
                    .stringType()
                    .defaultValue("TLSv1.3")
                    .withDescription(
                            "The SSL protocol version to be supported for the ssl transport. Note that it doesn’t"
                                    + " support comma separated list.");

    public static final ConfigOption<Boolean> ALLOW_INSECURE =
            ConfigOptions.key("security.allow-insecure")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allows to bypass the certificates chain validation and connect to insecure network endpoints");
}
