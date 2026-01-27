package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.connector.elasticsearch.NetworkClientConfig;
import org.apache.flink.connector.elasticsearch.lookup.ElasticsearchRowDataLookupFunction;
import org.apache.flink.connector.elasticsearch.table.search.ElasticsearchRowDataVectorSearchFunction;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.VectorSearchTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.connector.source.search.VectorSearchFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.elasticsearch.client.RestHighLevelClient;

import javax.annotation.Nullable;

/**
 * A {@link DynamicTableSource} that describes how to create a {@link Elasticsearch8DynamicSource}
 * from a logical description.
 */
public class Elasticsearch8DynamicSource implements VectorSearchTableSource, SupportsProjectionPushDown {

    protected final DecodingFormat<DeserializationSchema<RowData>> format;
    protected final ElasticsearchConfiguration config;
    protected final int maxRetryTimes;
    private final String summaryString;
    protected DataType physicalRowDataType;

    public Elasticsearch8DynamicSource(
            DecodingFormat<DeserializationSchema<RowData>> format,
            ElasticsearchConfiguration config,
            DataType physicalRowDataType,
            int maxRetryTimes,
            String summaryString) {
        this.format = format;
        this.config = config;
        this.physicalRowDataType = physicalRowDataType;
        this.maxRetryTimes = maxRetryTimes;
        this.summaryString = summaryString;
    }

    @SuppressWarnings("unchecked")
    @Override
    public VectorSearchRuntimeProvider getSearchRuntimeProvider(
            VectorSearchContext vectorSearchContext) {

        NetworkClientConfig networkClientConfig = buildNetworkClientConfig();

        ElasticsearchRowDataVectorSearchFunction vectorSearchFunction =
                new ElasticsearchRowDataVectorSearchFunction(
                        this.format.createRuntimeDecoder(vectorSearchContext, physicalRowDataType),
                        this.maxRetryTimes,
                        ((Elasticsearch8Configuration) config).getVectorSearchMetric(),
                        config.getIndex(),
                        getSearchColumn(vectorSearchContext),
                        DataType.getFieldNames(physicalRowDataType).toArray(new String[0]),
                        config.getHosts(),
                        networkClientConfig);

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

    protected NetworkClientConfig buildNetworkClientConfig() {
        NetworkClientConfig.Builder builder = new NetworkClientConfig.Builder();
        if (config.getUsername().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())) {
            builder.setUsername(config.getUsername().get());
        }

        if (config.getPassword().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get())) {
            builder.setPassword(config.getPassword().get());
        }

        if (config.getPathPrefix().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getPathPrefix().get())) {
            builder.setConnectionPathPrefix(config.getPathPrefix().get());
        }

        if (config.getConnectionRequestTimeout().isPresent()) {
            builder.setConnectionRequestTimeout(
                    (int) config.getConnectionRequestTimeout().get().getSeconds());
        }

        if (config.getConnectionTimeout().isPresent()) {
            builder.setConnectionTimeout((int) config.getConnectionTimeout().get().getSeconds());
        }

        if (config.getSocketTimeout().isPresent()) {
            builder.setSocketTimeout((int) config.getSocketTimeout().get().getSeconds());
        }

        return builder.build();
    }

    @Override
    public DynamicTableSource copy() {
        return new Elasticsearch8DynamicSource(
                format,
                config,
                physicalRowDataType,
                maxRetryTimes,
                summaryString);
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
