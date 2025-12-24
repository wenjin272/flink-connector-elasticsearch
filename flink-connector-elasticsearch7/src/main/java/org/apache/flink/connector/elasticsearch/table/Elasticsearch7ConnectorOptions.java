package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Options specific for the Elasticsearch 7 connector. Public so that the {@link
 * org.apache.flink.table.api.TableDescriptor} can access it.
 */
public class Elasticsearch7ConnectorOptions extends ElasticsearchConnectorOptions {
    private Elasticsearch7ConnectorOptions() {}

    public static final ConfigOption<Integer> VECTOR_SEARCH_MAX_RETRIES =
            ConfigOptions.key("vector-search.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The max retry times for vector searching Elasticsearch.");
}
