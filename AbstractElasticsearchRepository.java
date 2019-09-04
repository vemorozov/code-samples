package ru.cinimex.suv.adm.model.es.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import my.custom.FormatUtils;
import my.custom.ReflectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.search.sort.SortOrder.ASC;
import static org.elasticsearch.search.sort.SortOrder.DESC;

@Slf4j
public abstract class AbstractElasticsearchRepository<T> implements ElasticsearchRepository {

    private final RestHighLevelClient client;
    private final ObjectMapper mapper;
    private final Class<T> entityClass;
    private final String[] indices;

    public AbstractElasticsearchRepository(RestHighLevelClient client,
                                           ObjectMapper mapper,
                                           String... indices) {
        this.client = client;
        this.mapper = mapper;
        this.entityClass = ReflectionUtils.getGenericParameterClass(this.getClass(),
                                                                    AbstractElasticsearchRepository.class,
                                                                    0);
        this.indices = indices;
    }

    @Override
    public List<T> getByField(String name, Object value) {
        return getByQuery(matchPhraseQuery(name, value));
    }

    @Override
    public Page<T> getPageByQuery(QueryBuilder query, Pageable pageable) {
        return getSortedPage(query, pageable);
    }

    private List<T> getByQuery(QueryBuilder query) {
        return getByQuery(query, null);
    }

    private List<T> getByQuery(QueryBuilder query, Pageable pageable) {
        SearchRequest request = createSearchRequest(query, pageable);

        try {
            SearchResponse response = search(request);
            return mapResponse(response);
        } catch (IOException e) {
            log.error("Error searching in indices {}: {}", indices, e.toString());
            return emptyList();
        }
    }

    private Page<T> getSortedPage(QueryBuilder query, Pageable pageable) {
        long total = countMatches(query);
        if (total == 0) {
            return Page.empty();
        }

        List<T> responseList = getByQuery(query, pageable);

        return new PageImpl<>(responseList, pageable, total);
    }

    private long countMatches(QueryBuilder query) {
        CountRequest request = createCountRequest(query);

        try {
            CountResponse response = countMatches(request);

            return response.getCount();
        } catch (IOException e) {
            log.error("Error counting(indices: {}):\n{}", indices, e.toString());
            return 0;
        }
    }

    private CountResponse countMatches(CountRequest request) throws IOException {
        return client.count(request, RequestOptions.DEFAULT);
    }

    private SearchResponse search(SearchRequest request) throws IOException {
        return client.search(request, RequestOptions.DEFAULT);
    }

    private SearchRequest createSearchRequest(QueryBuilder query, Pageable pageable) {
        SearchRequest request = new SearchRequest(indices);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query);

        if (pageable != null) {
            int from = (int) pageable.getOffset();
            int size = pageable.getPageSize();
            SortBuilder sort = parseSort(pageable.getSort());

            searchSourceBuilder.from(from);
            searchSourceBuilder.size(size);
            searchSourceBuilder.sort(sort);
        }

        return request.source(searchSourceBuilder);
    }

    private CountRequest createCountRequest(QueryBuilder query) {
        CountRequest request = new CountRequest(indices);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query);
        request.source(searchSourceBuilder);

        log.info("Search source : {}", searchSourceBuilder);
        log.debug("Search request: {}", request);

        return request;
    }

    private List<T> mapResponse(SearchResponse response) {
        List<T> results = new ArrayList<>();

        stream(response.getHits().getHits())
            .forEach(hit -> {
                try {
                    results.add(
                        mapper.readValue(hit.getSourceAsString(), entityClass));
                } catch (IOException e) {
                    log.error("Error mapping results(indices: {}) to DTO:\n{}", indices, e.toString());
                }
            });

        return results;
    }

    private SortBuilder parseSort(Sort sort) {
        Order order = sort.get()
                          .findFirst()
                          .orElse(Order.asc("dateTime"));

        boolean asc = order.getDirection().isAscending();
        String javaFieldName = order.getProperty();
        String field = FormatUtils.camelCaseToUnderscore(javaFieldName);

        return new FieldSortBuilder(field).order(asc ? ASC : DESC);
    }
}
