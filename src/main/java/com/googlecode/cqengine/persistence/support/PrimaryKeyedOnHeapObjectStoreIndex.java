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
package com.googlecode.cqengine.persistence.support;

import com.googlecode.cqengine.index.Index;
import com.googlecode.cqengine.index.support.AbstractAttributeIndex;
import com.googlecode.cqengine.index.support.CloseableIterable;
import com.googlecode.cqengine.index.support.CloseableIterator;
import com.googlecode.cqengine.index.support.KeyStatistics;
import com.googlecode.cqengine.index.support.KeyValue;
import com.googlecode.cqengine.index.support.KeyValueMaterialized;
import com.googlecode.cqengine.index.support.SortedKeyStatisticsAttributeIndex;
import com.googlecode.cqengine.index.support.indextype.OnHeapTypeIndex;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.query.simple.Between;
import com.googlecode.cqengine.query.simple.Equal;
import com.googlecode.cqengine.query.simple.GreaterThan;
import com.googlecode.cqengine.query.simple.Has;
import com.googlecode.cqengine.query.simple.In;
import com.googlecode.cqengine.query.simple.LessThan;
import com.googlecode.cqengine.resultset.ResultSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * A lightweight read-only index view over a {@link PrimaryKeyedOnHeapObjectStore}.
 *
 * @author niall.gallagher
 */
public class PrimaryKeyedOnHeapObjectStoreIndex<O, A extends Comparable<A>> extends AbstractAttributeIndex<A, O> implements SortedKeyStatisticsAttributeIndex<A, O>, OnHeapTypeIndex {

    static final int INDEX_RETRIEVAL_COST = 40;

    final PrimaryKeyedOnHeapObjectStore<O, A> objectStore;

    public PrimaryKeyedOnHeapObjectStoreIndex(PrimaryKeyedOnHeapObjectStore<O, A> objectStore) {
        super(objectStore.getPrimaryKeyAttribute(), new HashSet<Class<? extends Query>>() {{
            add(Equal.class);
            add(In.class);
            add(LessThan.class);
            add(GreaterThan.class);
            add(Between.class);
            add(Has.class);
        }});
        this.objectStore = objectStore;
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public boolean isQuantized() {
        return false;
    }

    @Override
    public Index<O> getEffectiveIndex() {
        return this;
    }

    @Override
    public ResultSet<O> retrieve(final Query<O> query, final QueryOptions queryOptions) {
        final Class<?> queryClass = query.getClass();
        if (queryClass.equals(Equal.class)) {
            @SuppressWarnings("unchecked")
            final Equal<O, A> equal = (Equal<O, A>) query;
            return resultSetForSingleKey(equal.getValue(), query, queryOptions);
        }
        if (queryClass.equals(In.class)) {
            @SuppressWarnings("unchecked")
            final In<O, A> in = (In<O, A>) query;
            return resultSetForKeys(sortedValues(in.getValues()), query, queryOptions);
        }
        if (queryClass.equals(Has.class)) {
            return resultSetForMapValues(objectStore.backingMap, query, queryOptions);
        }
        if (queryClass.equals(LessThan.class)) {
            @SuppressWarnings("unchecked")
            final LessThan<O, A> lessThan = (LessThan<O, A>) query;
            return resultSetForMapValues(objectStore.backingMap.headMap(lessThan.getValue(), lessThan.isValueInclusive()), query, queryOptions);
        }
        if (queryClass.equals(GreaterThan.class)) {
            @SuppressWarnings("unchecked")
            final GreaterThan<O, A> greaterThan = (GreaterThan<O, A>) query;
            return resultSetForMapValues(objectStore.backingMap.tailMap(greaterThan.getValue(), greaterThan.isValueInclusive()), query, queryOptions);
        }
        if (queryClass.equals(Between.class)) {
            @SuppressWarnings("unchecked")
            final Between<O, A> between = (Between<O, A>) query;
            return resultSetForMapValues(
                    objectStore.backingMap.subMap(
                            between.getLowerValue(),
                            between.isLowerInclusive(),
                            between.getUpperValue(),
                            between.isUpperInclusive()
                    ),
                    query,
                    queryOptions
            );
        }
        throw new IllegalStateException("Unsupported query: " + query);
    }

    @Override
    public boolean addAll(ObjectSet<O> objectSet, QueryOptions queryOptions) {
        return false;
    }

    @Override
    public boolean removeAll(ObjectSet<O> objectSet, QueryOptions queryOptions) {
        return false;
    }

    @Override
    public void clear(QueryOptions queryOptions) {
        // No op.
    }

    @Override
    public void init(ObjectStore<O> objectStore, QueryOptions queryOptions) {
        if (objectStore != this.objectStore) {
            throw new IllegalStateException("This index must be initialized with its backing PrimaryKeyedOnHeapObjectStore.");
        }
    }

    @Override
    public void destroy(QueryOptions queryOptions) {
        // No op.
    }

    @Override
    public CloseableIterable<A> getDistinctKeys(QueryOptions queryOptions) {
        return distinctKeysIterable(objectStore.backingMap);
    }

    @Override
    public Integer getCountForKey(A key, QueryOptions queryOptions) {
        return objectStore.backingMap.containsKey(key) ? 1 : 0;
    }

    @Override
    public CloseableIterable<A> getDistinctKeys(A lowerBound, boolean lowerInclusive, A upperBound, boolean upperInclusive, QueryOptions queryOptions) {
        return distinctKeysIterable(mapRange(lowerBound, lowerInclusive, upperBound, upperInclusive, false));
    }

    @Override
    public CloseableIterable<A> getDistinctKeysDescending(QueryOptions queryOptions) {
        return distinctKeysIterable(objectStore.backingMap.descendingMap());
    }

    @Override
    public CloseableIterable<A> getDistinctKeysDescending(A lowerBound, boolean lowerInclusive, A upperBound, boolean upperInclusive, QueryOptions queryOptions) {
        return distinctKeysIterable(mapRange(lowerBound, lowerInclusive, upperBound, upperInclusive, true));
    }

    @Override
    public CloseableIterable<KeyStatistics<A>> getStatisticsForDistinctKeysDescending(QueryOptions queryOptions) {
        return keyStatisticsIterable(objectStore.backingMap.descendingMap());
    }

    @Override
    public Integer getCountOfDistinctKeys(QueryOptions queryOptions) {
        return objectStore.backingMap.size();
    }

    @Override
    public CloseableIterable<KeyStatistics<A>> getStatisticsForDistinctKeys(QueryOptions queryOptions) {
        return keyStatisticsIterable(objectStore.backingMap);
    }

    @Override
    public CloseableIterable<KeyValue<A, O>> getKeysAndValues(QueryOptions queryOptions) {
        return keysAndValuesIterable(objectStore.backingMap);
    }

    @Override
    public CloseableIterable<KeyValue<A, O>> getKeysAndValues(A lowerBound, boolean lowerInclusive, A upperBound, boolean upperInclusive, QueryOptions queryOptions) {
        return keysAndValuesIterable(mapRange(lowerBound, lowerInclusive, upperBound, upperInclusive, false));
    }

    @Override
    public CloseableIterable<KeyValue<A, O>> getKeysAndValuesDescending(QueryOptions queryOptions) {
        return keysAndValuesIterable(objectStore.backingMap.descendingMap());
    }

    @Override
    public CloseableIterable<KeyValue<A, O>> getKeysAndValuesDescending(A lowerBound, boolean lowerInclusive, A upperBound, boolean upperInclusive, QueryOptions queryOptions) {
        return keysAndValuesIterable(mapRange(lowerBound, lowerInclusive, upperBound, upperInclusive, true));
    }

    ConcurrentNavigableMap<A, O> mapRange(A lowerBound, boolean lowerInclusive, A upperBound, boolean upperInclusive, boolean descending) {
        ConcurrentNavigableMap<A, O> map = objectStore.backingMap;
        if (lowerBound != null && upperBound != null) {
            map = map.subMap(lowerBound, lowerInclusive, upperBound, upperInclusive);
        }
        else if (lowerBound != null) {
            map = map.tailMap(lowerBound, lowerInclusive);
        }
        else if (upperBound != null) {
            map = map.headMap(upperBound, upperInclusive);
        }
        return descending ? map.descendingMap() : map;
    }

    ResultSet<O> resultSetForMapValues(final ConcurrentNavigableMap<A, O> mapView, final Query<O> query, final QueryOptions queryOptions) {
        return new ResultSet<O>() {
            @Override
            public Iterator<O> iterator() {
                return mapView.values().iterator();
            }

            @Override
            public boolean contains(O object) {
                return containsStoredObjectMatchingQuery(object, query, queryOptions);
            }

            @Override
            public boolean matches(O object) {
                return query.matches(object, queryOptions);
            }

            @Override
            public Query<O> getQuery() {
                return query;
            }

            @Override
            public QueryOptions getQueryOptions() {
                return queryOptions;
            }

            @Override
            public int getRetrievalCost() {
                return INDEX_RETRIEVAL_COST;
            }

            @Override
            public int getMergeCost() {
                return mapView.size();
            }

            @Override
            public int size() {
                return mapView.size();
            }

            @Override
            public void close() {
                // No op.
            }
        };
    }

    ResultSet<O> resultSetForSingleKey(final A key, final Query<O> query, final QueryOptions queryOptions) {
        return new ResultSet<O>() {
            @Override
            public Iterator<O> iterator() {
                final O value = objectStore.backingMap.get(key);
                return value == null ? Collections.<O>emptyList().iterator() : Collections.singletonList(value).iterator();
            }

            @Override
            public boolean contains(O object) {
                return containsStoredObjectMatchingQuery(object, query, queryOptions);
            }

            @Override
            public boolean matches(O object) {
                return query.matches(object, queryOptions);
            }

            @Override
            public Query<O> getQuery() {
                return query;
            }

            @Override
            public QueryOptions getQueryOptions() {
                return queryOptions;
            }

            @Override
            public int getRetrievalCost() {
                return INDEX_RETRIEVAL_COST;
            }

            @Override
            public int getMergeCost() {
                return objectStore.backingMap.containsKey(key) ? 1 : 0;
            }

            @Override
            public int size() {
                return objectStore.backingMap.containsKey(key) ? 1 : 0;
            }

            @Override
            public void close() {
                // No op.
            }
        };
    }

    ResultSet<O> resultSetForKeys(final List<A> keys, final Query<O> query, final QueryOptions queryOptions) {
        return new ResultSet<O>() {
            @Override
            public Iterator<O> iterator() {
                return new Iterator<O>() {
                    final Iterator<A> keysIterator = keys.iterator();
                    O nextObject = null;

                    @Override
                    public boolean hasNext() {
                        while (nextObject == null && keysIterator.hasNext()) {
                            nextObject = objectStore.backingMap.get(keysIterator.next());
                        }
                        return nextObject != null;
                    }

                    @Override
                    public O next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        final O currentObject = nextObject;
                        nextObject = null;
                        return currentObject;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public boolean contains(O object) {
                return containsStoredObjectMatchingQuery(object, query, queryOptions);
            }

            @Override
            public boolean matches(O object) {
                return query.matches(object, queryOptions);
            }

            @Override
            public Query<O> getQuery() {
                return query;
            }

            @Override
            public QueryOptions getQueryOptions() {
                return queryOptions;
            }

            @Override
            public int getRetrievalCost() {
                return INDEX_RETRIEVAL_COST;
            }

            @Override
            public int getMergeCost() {
                return countPresentKeys(keys);
            }

            @Override
            public int size() {
                return countPresentKeys(keys);
            }

            @Override
            public void close() {
                // No op.
            }
        };
    }

    boolean containsStoredObjectMatchingQuery(O object, Query<O> query, QueryOptions queryOptions) {
        final O storedObject = objectStore.getObjectByPrimaryKey(object, queryOptions);
        return storedObject != null && query.matches(storedObject, queryOptions);
    }

    int countPresentKeys(List<A> keys) {
        int count = 0;
        for (A key : keys) {
            if (objectStore.backingMap.containsKey(key)) {
                count++;
            }
        }
        return count;
    }

    List<A> sortedValues(Iterable<A> values) {
        final List<A> sortedValues = new ArrayList<A>();
        for (A value : values) {
            if (value != null) {
                sortedValues.add(value);
            }
        }
        Collections.sort(sortedValues);
        return sortedValues;
    }

    CloseableIterable<A> distinctKeysIterable(final ConcurrentNavigableMap<A, O> mapView) {
        return new CloseableIterable<A>() {
            @Override
            public CloseableIterator<A> iterator() {
                return closeableIterator(mapView.navigableKeySet().iterator());
            }
        };
    }

    CloseableIterable<KeyStatistics<A>> keyStatisticsIterable(final ConcurrentNavigableMap<A, O> mapView) {
        return new CloseableIterable<KeyStatistics<A>>() {
            @Override
            public CloseableIterator<KeyStatistics<A>> iterator() {
                final Iterator<A> iterator = mapView.navigableKeySet().iterator();
                return new CloseableIterator<KeyStatistics<A>>() {
                    @Override
                    public void close() {
                        // No op.
                    }

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public KeyStatistics<A> next() {
                        return new KeyStatistics<A>(iterator.next(), 1);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    CloseableIterable<KeyValue<A, O>> keysAndValuesIterable(final ConcurrentNavigableMap<A, O> mapView) {
        return new CloseableIterable<KeyValue<A, O>>() {
            @Override
            public CloseableIterator<KeyValue<A, O>> iterator() {
                final Iterator<Map.Entry<A, O>> iterator = mapView.entrySet().iterator();
                return new CloseableIterator<KeyValue<A, O>>() {
                    @Override
                    public void close() {
                        // No op.
                    }

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public KeyValue<A, O> next() {
                        final Map.Entry<A, O> entry = iterator.next();
                        return new KeyValueMaterialized<A, O>(entry.getKey(), entry.getValue());
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    <T> CloseableIterator<T> closeableIterator(final Iterator<T> iterator) {
        return new CloseableIterator<T>() {
            @Override
            public void close() {
                // No op.
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return iterator.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
