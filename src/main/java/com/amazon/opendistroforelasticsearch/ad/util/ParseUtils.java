/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.util;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.QUERY_PARAM_PERIOD_END;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.QUERY_PARAM_PERIOD_START;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateRange;
import static org.elasticsearch.search.aggregations.AggregatorFactories.VALID_AGG_NAME;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.amazon.opendistroforelasticsearch.ad.model.FeatureData;
import com.amazon.opendistroforelasticsearch.commons.ConfigConstants;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;

/**
 * Parsing utility functions.
 */
public final class ParseUtils {
    private static Logger logger = LogManager.getLogger(ParseUtils.class);

    private ParseUtils() {}

    /**
     * Parse content parser to {@link java.time.Instant}.
     *
     * @param parser json based content parser
     * @return instance of {@link java.time.Instant}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static Instant toInstant(XContentParser parser) throws IOException {
        if (parser.currentToken() == null || parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        if (parser.currentToken().isValue()) {
            return Instant.ofEpochMilli(parser.longValue());
        }
        return null;
    }

    /**
     * Parse content parser to {@link AggregationBuilder}.
     *
     * @param parser json based content parser
     * @return instance of {@link AggregationBuilder}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AggregationBuilder toAggregationBuilder(XContentParser parser) throws IOException {
        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        return parsed.getAggregatorFactories().iterator().next();
    }

    /**
     * Parse json String into {@link XContentParser}.
     *
     * @param content         json string
     * @param contentRegistry ES named content registry
     * @return instance of {@link XContentParser}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static XContentParser parser(String content, NamedXContentRegistry contentRegistry) throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(contentRegistry, LoggingDeprecationHandler.INSTANCE, content);
        parser.nextToken();
        return parser;
    }

    /**
     * parse aggregation String into {@link AggregatorFactories.Builder}.
     *
     * @param aggQuery         aggregation query string
     * @param xContentRegistry ES named content registry
     * @param aggName          aggregation name, if set, will use it to replace original aggregation name
     * @return instance of {@link AggregatorFactories.Builder}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AggregatorFactories.Builder parseAggregators(String aggQuery, NamedXContentRegistry xContentRegistry, String aggName)
        throws IOException {
        XContentParser parser = parser(aggQuery, xContentRegistry);
        return parseAggregators(parser, aggName);
    }

    /**
     * Parse content parser to {@link AggregatorFactories.Builder}.
     *
     * @param parser  json based content parser
     * @param aggName aggregation name, if set, will use it to replace original aggregation name
     * @return instance of {@link AggregatorFactories.Builder}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AggregatorFactories.Builder parseAggregators(XContentParser parser, String aggName) throws IOException {
        return parseAggregators(parser, 0, aggName);
    }

    /**
     * Parse content parser to {@link AggregatorFactories.Builder}.
     *
     * @param parser  json based content parser
     * @param level   aggregation level, the top level start from 0
     * @param aggName aggregation name, if set, will use it to replace original aggregation name
     * @return instance of {@link AggregatorFactories.Builder}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static AggregatorFactories.Builder parseAggregators(XContentParser parser, int level, String aggName) throws IOException {
        Matcher validAggMatcher = VALID_AGG_NAME.matcher("");
        AggregatorFactories.Builder factories = new AggregatorFactories.Builder();

        XContentParser.Token token = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Unexpected token " + token + " in [aggs]: aggregations definitions must start with the name of the aggregation."
                );
            }
            final String aggregationName = aggName == null ? parser.currentName() : aggName;
            if (!validAggMatcher.reset(aggregationName).matches()) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Invalid aggregation name ["
                        + aggregationName
                        + "]. Aggregation names must be alpha-numeric and can only contain '_' and '-'"
                );
            }

            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Aggregation definition for ["
                        + aggregationName
                        + " starts with a ["
                        + token
                        + "], expected a ["
                        + XContentParser.Token.START_OBJECT
                        + "]."
                );
            }

            BaseAggregationBuilder aggBuilder = null;
            AggregatorFactories.Builder subFactories = null;

            Map<String, Object> metaData = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Expected ["
                            + XContentParser.Token.FIELD_NAME
                            + "] under a ["
                            + XContentParser.Token.START_OBJECT
                            + "], but got a ["
                            + token
                            + "] in ["
                            + aggregationName
                            + "]",
                        parser.getTokenLocation()
                    );
                }
                final String fieldName = parser.currentName();

                token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    switch (fieldName) {
                        case "meta":
                            metaData = parser.map();
                            break;
                        case "aggregations":
                        case "aggs":
                            if (subFactories != null) {
                                throw new ParsingException(
                                    parser.getTokenLocation(),
                                    "Found two sub aggregation definitions under [" + aggregationName + "]"
                                );
                            }
                            subFactories = parseAggregators(parser, level + 1, null);
                            break;
                        default:
                            if (aggBuilder != null) {
                                throw new ParsingException(
                                    parser.getTokenLocation(),
                                    "Found two aggregation type definitions in ["
                                        + aggregationName
                                        + "]: ["
                                        + aggBuilder.getType()
                                        + "] and ["
                                        + fieldName
                                        + "]"
                                );
                            }

                            aggBuilder = parser.namedObject(BaseAggregationBuilder.class, fieldName, aggregationName);
                    }
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Expected ["
                            + XContentParser.Token.START_OBJECT
                            + "] under ["
                            + fieldName
                            + "], but got a ["
                            + token
                            + "] in ["
                            + aggregationName
                            + "]"
                    );
                }
            }

            if (aggBuilder == null) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Missing definition for aggregation [" + aggregationName + "]",
                    parser.getTokenLocation()
                );
            } else {
                if (metaData != null) {
                    aggBuilder.setMetadata(metaData);
                }

                if (subFactories != null) {
                    aggBuilder.subAggregations(subFactories);
                }

                if (aggBuilder instanceof AggregationBuilder) {
                    factories.addAggregator((AggregationBuilder) aggBuilder);
                } else {
                    factories.addPipelineAggregator((PipelineAggregationBuilder) aggBuilder);
                }
            }
        }

        return factories;
    }

    public static SearchSourceBuilder generateInternalFeatureQuery(
        AnomalyDetector detector,
        long startTime,
        long endTime,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(detector.getTimeField())
            .from(startTime)
            .to(endTime)
            .format("epoch_millis")
            .includeLower(true)
            .includeUpper(false);

        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(rangeQuery).must(detector.getFilterQuery());

        SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(internalFilterQuery);
        if (detector.getFeatureAttributes() != null) {
            for (Feature feature : detector.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = parseAggregators(
                    feature.getAggregation().toString(),
                    xContentRegistry,
                    feature.getId()
                );
                internalSearchSourceBuilder.aggregation(internalAgg.getAggregatorFactories().iterator().next());
            }
        }

        return internalSearchSourceBuilder;
    }

    public static SearchSourceBuilder generatePreviewQuery(
        AnomalyDetector detector,
        List<Entry<Long, Long>> ranges,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {

        DateRangeAggregationBuilder dateRangeBuilder = dateRange("date_range").field(detector.getTimeField()).format("epoch_millis");
        for (Entry<Long, Long> range : ranges) {
            dateRangeBuilder.addRange(range.getKey(), range.getValue());
        }

        if (detector.getFeatureAttributes() != null) {
            for (Feature feature : detector.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = parseAggregators(
                    feature.getAggregation().toString(),
                    xContentRegistry,
                    feature.getId()
                );
                dateRangeBuilder.subAggregation(internalAgg.getAggregatorFactories().iterator().next());
            }
        }

        return new SearchSourceBuilder().query(detector.getFilterQuery()).size(0).aggregation(dateRangeBuilder);
    }

    public static String generateInternalFeatureQueryTemplate(AnomalyDetector detector, NamedXContentRegistry xContentRegistry)
        throws IOException {
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(detector.getTimeField())
            .from("{{" + QUERY_PARAM_PERIOD_START + "}}")
            .to("{{" + QUERY_PARAM_PERIOD_END + "}}");

        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().must(rangeQuery).must(detector.getFilterQuery());

        SearchSourceBuilder internalSearchSourceBuilder = new SearchSourceBuilder().query(internalFilterQuery);
        if (detector.getFeatureAttributes() != null) {
            for (Feature feature : detector.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = parseAggregators(
                    feature.getAggregation().toString(),
                    xContentRegistry,
                    feature.getId()
                );
                internalSearchSourceBuilder.aggregation(internalAgg.getAggregatorFactories().iterator().next());
            }
        }

        return internalSearchSourceBuilder.toString();
    }

    public static SearchSourceBuilder generateEntityColdStartQuery(
        AnomalyDetector detector,
        List<Entry<Long, Long>> ranges,
        String entityName,
        NamedXContentRegistry xContentRegistry
    ) throws IOException {

        TermQueryBuilder term = new TermQueryBuilder(detector.getCategoryField().get(0), entityName);
        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().filter(detector.getFilterQuery()).filter(term);

        DateRangeAggregationBuilder dateRangeBuilder = dateRange("date_range").field(detector.getTimeField()).format("epoch_millis");
        for (Entry<Long, Long> range : ranges) {
            dateRangeBuilder.addRange(range.getKey(), range.getValue());
        }

        if (detector.getFeatureAttributes() != null) {
            for (Feature feature : detector.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = parseAggregators(
                    feature.getAggregation().toString(),
                    xContentRegistry,
                    feature.getId()
                );
                dateRangeBuilder.subAggregation(internalAgg.getAggregatorFactories().iterator().next());
            }
        }

        return new SearchSourceBuilder().query(internalFilterQuery).size(0).aggregation(dateRangeBuilder);
    }

    /**
     * Map feature data to its Id and name
     * @param currentFeature Feature data
     * @param detector Detector Config object
     * @return a list of feature data with Id and name
     */
    public static List<FeatureData> getFeatureData(double[] currentFeature, AnomalyDetector detector) {
        List<String> featureIds = detector.getEnabledFeatureIds();
        List<String> featureNames = detector.getEnabledFeatureNames();
        int featureLen = featureIds.size();
        List<FeatureData> featureData = new ArrayList<>();
        for (int i = 0; i < featureLen; i++) {
            featureData.add(new FeatureData(featureIds.get(i), featureNames.get(i), currentFeature[i]));
        }
        return featureData;
    }

    public static SearchSourceBuilder addUserBackendRolesFilter(User user, SearchSourceBuilder searchSourceBuilder) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        String userFieldName = "user";
        String userBackendRoleFieldName = "user.backend_roles.keyword";
        if (user == null) {
            // For old monitor and detector, they have no user field, user = null
            ExistsQueryBuilder userRolesFilterQuery = QueryBuilders.existsQuery(userFieldName);
            NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(userFieldName, userRolesFilterQuery, ScoreMode.None);
            boolQueryBuilder.mustNot(nestedQueryBuilder);
        } else if (user.getBackendRoles() == null || user.getBackendRoles().size() == 0) {
            // For simple FGAC user, they may have no backend roles, these users should be able to see detectors
            // of other users whose backend role is empty. user != null, user.backend_role == null
            ExistsQueryBuilder userRolesFilterQuery = QueryBuilders.existsQuery(userBackendRoleFieldName);
            NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(userFieldName, userRolesFilterQuery, ScoreMode.None);

            ExistsQueryBuilder userExistsQuery = QueryBuilders.existsQuery(userFieldName);
            NestedQueryBuilder userExistsNestedQueryBuilder = new NestedQueryBuilder(userFieldName, userExistsQuery, ScoreMode.None);

            boolQueryBuilder.mustNot(nestedQueryBuilder);
            boolQueryBuilder.must(userExistsNestedQueryBuilder);
        } else {
            // For normal case, user should have backend roles.
            TermsQueryBuilder userRolesFilterQuery = QueryBuilders.termsQuery(userBackendRoleFieldName, user.getBackendRoles());
            NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(userFieldName, userRolesFilterQuery, ScoreMode.None);
            boolQueryBuilder.must(nestedQueryBuilder);
        }
        QueryBuilder query = searchSourceBuilder.query();
        if (query == null) {
            searchSourceBuilder.query(boolQueryBuilder);
        } else if (query instanceof BoolQueryBuilder) {
            ((BoolQueryBuilder) query).filter(boolQueryBuilder);
        } else {
            throw new ElasticsearchException("Search API does not support queries other than BoolQuery");
        }
        return searchSourceBuilder;
    }

    public static User getUserContext(Client client) {
        String userStr = client.threadPool().getThreadContext().getTransient(ConfigConstants.OPENDISTRO_SECURITY_USER_AND_ROLES);
        logger.debug("Filtering result by " + userStr);
        return User.parse(userStr);
    }
}
