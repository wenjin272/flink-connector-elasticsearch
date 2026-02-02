package org.apache.flink.connector.elasticsearch.table.search;

import co.elastic.clients.json.JsonData;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.elasticsearch.sink.NetworkConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.VectorSearchFunction;
import org.apache.flink.util.FlinkRuntimeException;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The {@link VectorSearchFunction} implementation for Elasticsearch. */
public class ElasticsearchRowDataVectorSearchFunction extends VectorSearchFunction {
    private static final Logger LOG =
            LoggerFactory.getLogger(ElasticsearchRowDataVectorSearchFunction.class);
    private static final long serialVersionUID = 1L;

    private final DeserializationSchema<RowData> deserializationSchema;

    private final String index;

    private final String[] producedNames;
    private final int maxRetryTimes;
    private final int numCandidates;
    private final String searchColumn;

    private final NetworkConfig networkConfig;

    private transient ElasticsearchClient client;

    public ElasticsearchRowDataVectorSearchFunction(
            DeserializationSchema<RowData> deserializationSchema,
            int maxRetryTimes,
            int numCandidates,
            String index,
            String searchColumn,
            String[] producedNames,
            NetworkConfig networkConfig) {

        checkNotNull(deserializationSchema, "No DeserializationSchema supplied.");
        checkNotNull(maxRetryTimes, "No maxRetryTimes supplied.");
        checkNotNull(producedNames, "No fieldNames supplied.");
        checkNotNull(networkConfig, "No networkConfig supplied.");

        this.deserializationSchema = deserializationSchema;
        this.maxRetryTimes = maxRetryTimes;
        this.numCandidates = numCandidates;
        this.index = index;
        this.searchColumn = searchColumn;
        this.producedNames = producedNames;
        this.networkConfig = networkConfig;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        this.client = networkConfig.createEsSyncClient();

        deserializationSchema.open(null);
    }

    @Override
    public Collection<RowData> vectorSearch(int topK, RowData features) throws IOException {
        List<Float> queryVector = new ArrayList<>();
        for (float feature : features.getArray(0).toFloatArray()) {
            queryVector.add(feature);
        }
        SearchRequest.Builder builder =
                new SearchRequest.Builder()
                        .index(index)
                        .knn(kb -> kb.field(searchColumn).numCandidates(numCandidates).queryVector(queryVector).k(topK))
                        .source(src -> src.filter(f -> f.includes(Arrays.asList(producedNames))));
        SearchRequest request = builder.build();

        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                ArrayList<RowData> rows = new ArrayList<>();
                Tuple2<String, SearchResult[]> searchResponse = search(client, request);

                if (searchResponse.f1.length > 0) {
                    for (SearchResult result : searchResponse.f1) {
                        String source = result.source;
                        RowData row = parseSearchResult(source);
                        GenericRowData scoreData = new GenericRowData(1);
                        scoreData.setField(0, result.score);
                        if (row != null) {
                            rows.add(new JoinedRowData(row, scoreData));
                        }
                    }
                    rows.trimToSize();
                    return rows;
                }
            } catch (IOException e) {
                LOG.error(String.format("Elasticsearch search error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new FlinkRuntimeException("Execution of Elasticsearch search failed.", e);
                }
                try {
                    Thread.sleep(1000L * retry);
                } catch (InterruptedException e1) {
                    LOG.warn(
                            "Interrupted while waiting to retry failed elasticsearch search, aborting");
                    throw new FlinkRuntimeException(e1);
                }
            }
        }
        return Collections.emptyList();
    }

    private RowData parseSearchResult(String result) {
        RowData row = null;
        try {
            row = deserializationSchema.deserialize(result.getBytes());
        } catch (IOException e) {
            LOG.error("Deserialize search hit failed: " + e.getMessage());
        }

        return row;
    }

    private Tuple2<String, SearchResult[]> search(
            ElasticsearchClient client, SearchRequest searchRequest) throws IOException {
        SearchResponse<JsonData> searchResponse = client.search(searchRequest, JsonData.class);
        List<Hit<JsonData>> searchHits = searchResponse.hits().hits();

        return new Tuple2<>(
                searchResponse.scrollId(),
                searchHits.stream()
                        .map(hit -> {
                            if (hit.source() != null) {
                                return new SearchResult(hit.source().toJson().toString(), hit.score());
                            } else {
                                return new SearchResult(null, hit.score());
                            }
                        })
                        .toArray(SearchResult[]::new));
    }

    private static class SearchResult {
        private final String source;
        private final Double score;

        public SearchResult(String source, Double score) {
            this.source = source;
            this.score = score;
        }
    }
}
