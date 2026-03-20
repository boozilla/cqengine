/**
 * Copyright 2012-2015 Niall Gallagher
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.googlecode.cqengine.engine;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.index.navigable.NavigableIndex;
import com.googlecode.cqengine.persistence.Persistence;
import com.googlecode.cqengine.persistence.onheap.OnHeapPersistence;
import com.googlecode.cqengine.query.option.OrderByOption;
import com.googlecode.cqengine.query.option.QueryLog;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.resultset.ResultSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.googlecode.cqengine.query.QueryFactory.all;
import static com.googlecode.cqengine.query.QueryFactory.applyThresholds;
import static com.googlecode.cqengine.query.QueryFactory.ascending;
import static com.googlecode.cqengine.query.QueryFactory.descending;
import static com.googlecode.cqengine.query.QueryFactory.noQueryOptions;
import static com.googlecode.cqengine.query.QueryFactory.orderBy;
import static com.googlecode.cqengine.query.QueryFactory.queryOptions;
import static com.googlecode.cqengine.query.QueryFactory.threshold;
import static com.googlecode.cqengine.query.option.EngineThresholds.INDEX_ORDERING_SELECTIVITY;

public class DeterministicPaginationOrderingTest {

    static final SimpleAttribute<TestRecord, Integer> RECORD_ID = new SimpleAttribute<TestRecord, Integer>(TestRecord.class, Integer.class, "recordId") {
        @Override
        public Integer getValue(TestRecord object, QueryOptions queryOptions) {
            return object.recordId;
        }
    };

    static final SimpleAttribute<TestRecord, Integer> GROUP = new SimpleAttribute<TestRecord, Integer>(TestRecord.class, Integer.class, "group") {
        @Override
        public Integer getValue(TestRecord object, QueryOptions queryOptions) {
            return object.group;
        }
    };

    @Test
    public void testMaterializedOrderingAppendsPrimaryKeyTieBreaker() {
        final IndexedCollection<TestRecord> collection = createPrimaryKeyedCollection();
        collection.addAll(Arrays.asList(
                new TestRecord(3, 1, 97),
                new TestRecord(2, 1, 98),
                new TestRecord(1, 1, 99)
        ));

        final ResultSet<TestRecord> resultSet = collection.retrieve(all(TestRecord.class), queryOptions(orderBy(ascending(GROUP))));
        try {
            Assert.assertEquals(Arrays.asList(1, 2, 3), extractRecordIds(resultSet));
        }
        finally {
            resultSet.close();
        }
    }

    @Test
    public void testIndexOrderingAppendsPrimaryKeyTieBreakerWithinBuckets() {
        final IndexedCollection<TestRecord> collection = createPrimaryKeyedCollection();
        collection.addIndex(NavigableIndex.onAttribute(GROUP));
        collection.addAll(Arrays.asList(
                new TestRecord(3, 1, 97),
                new TestRecord(2, 1, 98),
                new TestRecord(1, 1, 99)
        ));
        final StringBuilder queryLogSink = new StringBuilder();

        final ResultSet<TestRecord> resultSet = collection.retrieve(
                all(TestRecord.class),
                queryOptions(
                        orderBy(ascending(GROUP)),
                        applyThresholds(threshold(INDEX_ORDERING_SELECTIVITY, 1.0)),
                        new QueryLog(queryLogSink)
                )
        );
        try {
            Assert.assertEquals(Arrays.asList(1, 2, 3), extractRecordIds(resultSet));
        }
        finally {
            resultSet.close();
        }

        Assert.assertTrue(queryLogSink.toString().contains("orderingStrategy: index"));
    }

    @Test
    public void testIndexOrderingPreservesPrimaryKeyOrderAcrossMultipleBuckets() {
        final IndexedCollection<TestRecord> collection = createPrimaryKeyedCollection();
        collection.addIndex(NavigableIndex.onAttribute(GROUP));
        collection.addAll(Arrays.asList(
                new TestRecord(4, 2, 94),
                new TestRecord(2, 1, 98),
                new TestRecord(3, 2, 97),
                new TestRecord(1, 1, 99)
        ));

        final ResultSet<TestRecord> resultSet = collection.retrieve(
                all(TestRecord.class),
                queryOptions(orderBy(ascending(GROUP)), applyThresholds(threshold(INDEX_ORDERING_SELECTIVITY, 1.0)))
        );
        try {
            Assert.assertEquals(Arrays.asList(1, 2, 3, 4), extractRecordIds(resultSet));
        }
        finally {
            resultSet.close();
        }
    }

    @Test
    public void testResolveExplicitOrderByOptionDoesNotDuplicatePrimaryKey() {
        final CollectionQueryEngine<TestRecord> queryEngine = new CollectionQueryEngine<TestRecord>();
        queryEngine.init(emptyPrimaryKeyedObjectStore(), queryOptionsWithPrimaryKeyedPersistence());

        final OrderByOption<TestRecord> explicitOrderBy = orderBy(ascending(GROUP), descending(RECORD_ID));
        final OrderByOption<TestRecord> resolvedOrderBy = queryEngine.resolveExplicitOrderByOption(explicitOrderBy);

        Assert.assertEquals(explicitOrderBy, resolvedOrderBy);
    }

    static IndexedCollection<TestRecord> createPrimaryKeyedCollection() {
        return new ConcurrentIndexedCollection<TestRecord>(OnHeapPersistence.onPrimaryKey(RECORD_ID));
    }

    static com.googlecode.cqengine.persistence.support.ObjectStore<TestRecord> emptyPrimaryKeyedObjectStore() {
        return OnHeapPersistence.onPrimaryKey(RECORD_ID).createObjectStore();
    }

    static QueryOptions queryOptionsWithPrimaryKeyedPersistence() {
        final QueryOptions queryOptions = noQueryOptions();
        queryOptions.put(Persistence.class, OnHeapPersistence.onPrimaryKey(RECORD_ID));
        return queryOptions;
    }

    static List<Integer> extractRecordIds(ResultSet<TestRecord> resultSet) {
        return resultSet.stream().map(record -> record.recordId).collect(Collectors.toList());
    }

    static class TestRecord {
        final int recordId;
        final int group;
        final int syntheticHashCode;

        TestRecord(int recordId, int group, int syntheticHashCode) {
            this.recordId = recordId;
            this.group = group;
            this.syntheticHashCode = syntheticHashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestRecord)) return false;

            final TestRecord that = (TestRecord) o;
            return recordId == that.recordId;
        }

        @Override
        public int hashCode() {
            return syntheticHashCode;
        }
    }
}
