package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.configuration.ReadableConfig;

import static org.apache.flink.connector.elasticsearch.table.Elasticsearch7ConnectorOptions.VECTOR_SEARCH_MAX_RETRIES;

/** Elasticsearch 7 specific configuration. */
public class Elasticsearch7Configuration extends ElasticsearchConfiguration {
    Elasticsearch7Configuration(ReadableConfig config) {
        super(config);
    }

    public int getVectorSearchMaxRetries() {
        return config.get(VECTOR_SEARCH_MAX_RETRIES);
    }
}
