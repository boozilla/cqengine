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
package com.googlecode.cqengine.index.fallback;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.index.Index;
import com.googlecode.cqengine.persistence.support.ObjectSet;
import com.googlecode.cqengine.persistence.support.ObjectStore;
import com.googlecode.cqengine.query.ComparativeQuery;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.query.simple.*;
import com.googlecode.cqengine.resultset.ResultSet;
import com.googlecode.cqengine.resultset.iterator.IteratorUtil;
import com.googlecode.cqengine.resultset.iterator.UnmodifiableIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A special index which when asked to retrieve data simply scans the underlying collection for matching objects.
 * This index does not maintain any data structure of its own.
 * <p/>
 * This index supports <b>all</b> query types, because it it relies on the supplied query object itself
 * to determine if objects in the collection match the query, by calling
 * {@link Query#matches(Object, com.googlecode.cqengine.query.option.QueryOptions)}.
 * <p/>
 * The query engine automatically uses this <i>fallback</i> index when an attribute is referenced by a query,
 * and no other index has been added for that attribute that supports the query.
 * <p/>
 * The time complexity of retrievals from this fallback index is usually O(n) - linear, proportional to the number of
 * objects in the collection.
 *
 * @author Niall Gallagher
 */
public class FallbackIndex<O> implements Index<O> {

    private static final int INDEX_RETRIEVAL_COST = Integer.MAX_VALUE;
    private static final int INDEX_MERGE_COST = Integer.MAX_VALUE;

    volatile ObjectStore<O> objectStore = null;

    public FallbackIndex() {
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This index is mutable.
     *
     * @return true
     */
    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public Index<O> getEffectiveIndex() {
        return this;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * <b>This implementation always returns true, as this index supports all types of query.</b>
     *
     * @return true, this index supports all types of query
     */
    @Override
    public boolean supportsQuery(Query<O> query, QueryOptions queryOptions) {
        return true;
    }

    @Override
    public boolean isQuantized() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet<O> retrieve(final Query<O> query, final QueryOptions queryOptions) {
        final ObjectSet<O> objectSet = ObjectSet.fromObjectStore(objectStore, queryOptions);
        final ObjectMatcher<O> objectMatcher = query instanceof All || query instanceof None || query instanceof ComparativeQuery
                ? null
                : createObjectMatcher(query, queryOptions);
        return new ResultSet<O>() {
            @SuppressWarnings("unchecked")
            @Override
            public Iterator<O> iterator() {
                if (query instanceof All) {
                    return IteratorUtil.wrapAsUnmodifiable(objectSet.iterator());
                }
                else if (query instanceof None) {
                    return Collections.<O>emptyList().iterator();
                }
                else if (query instanceof ComparativeQuery) {
                    return ((ComparativeQuery<O, ?>)query).getMatches(objectSet, queryOptions).iterator();
                }
                else {
                    return new MatchingIterator<O>(objectSet.iterator(), objectMatcher);
                }
            }
            @Override
            public boolean contains(O object) {
                if (query instanceof ComparativeQuery) {
                    // Contains is based on objects contained in this *filtered* ResultSet, so delegate to iterator...
                    return IteratorUtil.iterableContains(this, object);
                }
                return objectStore.contains(object, queryOptions) && objectMatcher.matches(object);
            }
            @Override
            public int size() {
                if (query instanceof All) {
                    return objectStore.size(queryOptions);
                }
                if (query instanceof None) {
                    return 0;
                }
                if (query instanceof ComparativeQuery) {
                    // Size is based on objects contained in this *filtered* ResultSet, so delegate to iterator...
                    return IteratorUtil.countElements(this);
                }
                final ObjectSet<O> countObjectSet = ObjectSet.fromObjectStore(objectStore, queryOptions);
                try {
                    int count = 0;
                    for (final O object : countObjectSet) {
                        if (objectMatcher.matches(object)) {
                            count++;
                        }
                    }
                    return count;
                }
                finally {
                    countObjectSet.close();
                }
            }
            @Override
            public boolean matches(O object) {
                return query instanceof ComparativeQuery ? contains(object) : objectMatcher.matches(object);
            }
            @Override
            public int getRetrievalCost() {
                // None is a special case where we know it can't match any objects, and therefore retrieval cost is 0...
                return query instanceof None ? 0 : INDEX_RETRIEVAL_COST;
            }
            @Override
            public int getMergeCost() {
                // None is a special case where we know it can't match any objects, and therefore merge cost is 0...
                return query instanceof None ? 0 : INDEX_MERGE_COST;
            }
            @Override
            public void close() {
                objectSet.close();
            }
            @Override
            public Query<O> getQuery() {
                return query;
            }
            @Override
            public QueryOptions getQueryOptions() {
                return queryOptions;
            }
        };
    }

    static <O> ObjectMatcher<O> createObjectMatcher(final Query<O> query, final QueryOptions queryOptions) {
        if (!(query instanceof SimpleQuery)) {
            return new ObjectMatcher<O>() {
                @Override
                public boolean matches(O object) {
                    return query.matches(object, queryOptions);
                }
            };
        }
        final SimpleQuery<O, ?> simpleQuery = (SimpleQuery<O, ?>) query;
        if (!(simpleQuery.getAttribute() instanceof SimpleAttribute)) {
            return new ObjectMatcher<O>() {
                @Override
                public boolean matches(O object) {
                    return query.matches(object, queryOptions);
                }
            };
        }
        if (query instanceof Equal) {
            return createEqualMatcher((Equal<O, ?>) query, queryOptions);
        }
        if (query instanceof In) {
            return createInMatcher((In<O, ?>) query, queryOptions);
        }
        if (query instanceof Has) {
            return createHasMatcher((Has<O, ?>) query, queryOptions);
        }
        if (query instanceof LessThan) {
            return createLessThanMatcher((LessThan<O, ? extends Comparable>) query, queryOptions);
        }
        if (query instanceof GreaterThan) {
            return createGreaterThanMatcher((GreaterThan<O, ? extends Comparable>) query, queryOptions);
        }
        if (query instanceof Between) {
            return createBetweenMatcher((Between<O, ? extends Comparable>) query, queryOptions);
        }
        if (query instanceof StringStartsWith) {
            return createStringStartsWithMatcher((StringStartsWith<O, ? extends CharSequence>) query, queryOptions);
        }
        if (query instanceof StringEndsWith) {
            return createStringEndsWithMatcher((StringEndsWith<O, ? extends CharSequence>) query, queryOptions);
        }
        return new ObjectMatcher<O>() {
            @Override
            public boolean matches(O object) {
                return query.matches(object, queryOptions);
            }
        };
    }

    static <O, A> ObjectMatcher<O> createEqualMatcher(final Equal<O, A> equal, final QueryOptions queryOptions) {
        @SuppressWarnings("unchecked")
        final SimpleAttribute<O, A> attribute = (SimpleAttribute<O, A>) equal.getAttribute();
        final A value = equal.getValue();
        return new ObjectMatcher<O>() {
            @Override
            public boolean matches(O object) {
                return value.equals(attribute.getValue(object, queryOptions));
            }
        };
    }

    static <O, A> ObjectMatcher<O> createInMatcher(final In<O, A> in, final QueryOptions queryOptions) {
        @SuppressWarnings("unchecked")
        final SimpleAttribute<O, A> attribute = (SimpleAttribute<O, A>) in.getAttribute();
        final Set<A> values = in.getValues();
        return new ObjectMatcher<O>() {
            @Override
            public boolean matches(O object) {
                return values.contains(attribute.getValue(object, queryOptions));
            }
        };
    }

    static <O, A> ObjectMatcher<O> createHasMatcher(final Has<O, A> has, final QueryOptions queryOptions) {
        @SuppressWarnings("unchecked")
        final SimpleAttribute<O, A> attribute = (SimpleAttribute<O, A>) has.getAttribute();
        return new ObjectMatcher<O>() {
            @Override
            public boolean matches(O object) {
                return attribute.getValue(object, queryOptions) != null;
            }
        };
    }

    static <O, A extends Comparable<A>> ObjectMatcher<O> createLessThanMatcher(final LessThan<O, A> lessThan, final QueryOptions queryOptions) {
        @SuppressWarnings("unchecked")
        final SimpleAttribute<O, A> attribute = (SimpleAttribute<O, A>) lessThan.getAttribute();
        final A value = lessThan.getValue();
        final boolean inclusive = lessThan.isValueInclusive();
        return new ObjectMatcher<O>() {
            @Override
            public boolean matches(O object) {
                final A attributeValue = attribute.getValue(object, queryOptions);
                return inclusive ? value.compareTo(attributeValue) >= 0 : value.compareTo(attributeValue) > 0;
            }
        };
    }

    static <O, A extends Comparable<A>> ObjectMatcher<O> createGreaterThanMatcher(final GreaterThan<O, A> greaterThan, final QueryOptions queryOptions) {
        @SuppressWarnings("unchecked")
        final SimpleAttribute<O, A> attribute = (SimpleAttribute<O, A>) greaterThan.getAttribute();
        final A value = greaterThan.getValue();
        final boolean inclusive = greaterThan.isValueInclusive();
        return new ObjectMatcher<O>() {
            @Override
            public boolean matches(O object) {
                final A attributeValue = attribute.getValue(object, queryOptions);
                return inclusive ? value.compareTo(attributeValue) <= 0 : value.compareTo(attributeValue) < 0;
            }
        };
    }

    static <O, A extends Comparable<A>> ObjectMatcher<O> createBetweenMatcher(final Between<O, A> between, final QueryOptions queryOptions) {
        @SuppressWarnings("unchecked")
        final SimpleAttribute<O, A> attribute = (SimpleAttribute<O, A>) between.getAttribute();
        final A lowerValue = between.getLowerValue();
        final A upperValue = between.getUpperValue();
        final boolean lowerInclusive = between.isLowerInclusive();
        final boolean upperInclusive = between.isUpperInclusive();
        return new ObjectMatcher<O>() {
            @Override
            public boolean matches(O object) {
                final A attributeValue = attribute.getValue(object, queryOptions);
                if (lowerInclusive && upperInclusive) {
                    return lowerValue.compareTo(attributeValue) <= 0 && upperValue.compareTo(attributeValue) >= 0;
                }
                if (lowerInclusive) {
                    return lowerValue.compareTo(attributeValue) <= 0 && upperValue.compareTo(attributeValue) > 0;
                }
                if (upperInclusive) {
                    return lowerValue.compareTo(attributeValue) < 0 && upperValue.compareTo(attributeValue) >= 0;
                }
                return lowerValue.compareTo(attributeValue) < 0 && upperValue.compareTo(attributeValue) > 0;
            }
        };
    }

    static <O, A extends CharSequence> ObjectMatcher<O> createStringStartsWithMatcher(final StringStartsWith<O, A> startsWith, final QueryOptions queryOptions) {
        @SuppressWarnings("unchecked")
        final SimpleAttribute<O, A> attribute = (SimpleAttribute<O, A>) startsWith.getAttribute();
        final A value = startsWith.getValue();
        return new ObjectMatcher<O>() {
            @Override
            public boolean matches(O object) {
                return StringStartsWith.matchesValue(attribute.getValue(object, queryOptions), value, queryOptions);
            }
        };
    }

    static <O, A extends CharSequence> ObjectMatcher<O> createStringEndsWithMatcher(final StringEndsWith<O, A> endsWith, final QueryOptions queryOptions) {
        @SuppressWarnings("unchecked")
        final SimpleAttribute<O, A> attribute = (SimpleAttribute<O, A>) endsWith.getAttribute();
        return new ObjectMatcher<O>() {
            @Override
            public boolean matches(O object) {
                return endsWith.matchesValue(attribute.getValue(object, queryOptions), queryOptions);
            }
        };
    }

    interface ObjectMatcher<O> {
        boolean matches(O object);
    }

    static class MatchingIterator<O> extends UnmodifiableIterator<O> {
        final Iterator<O> wrappedIterator;
        final ObjectMatcher<O> objectMatcher;

        O nextObject = null;
        boolean nextObjectIsNull = false;
        boolean finished = false;

        MatchingIterator(Iterator<O> wrappedIterator, ObjectMatcher<O> objectMatcher) {
            this.wrappedIterator = wrappedIterator;
            this.objectMatcher = objectMatcher;
        }

        @Override
        public boolean hasNext() {
            if (finished) {
                return false;
            }
            if (nextObjectIsNull || nextObject != null) {
                return true;
            }
            while (wrappedIterator.hasNext()) {
                nextObject = wrappedIterator.next();
                if (objectMatcher.matches(nextObject)) {
                    nextObjectIsNull = (nextObject == null);
                    return true;
                }
                nextObjectIsNull = false;
            }
            finished = true;
            return false;
        }

        @Override
        public O next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final O objectToReturn = nextObject;
            nextObject = null;
            nextObjectIsNull = false;
            return objectToReturn;
        }
    }

    /**
     * {@inheritDoc}
     * <p/>
     * <b>In this implementation, does nothing.</b>
     */
    @Override
    public boolean addAll(ObjectSet<O> objectSet, QueryOptions queryOptions) {
        // No need to take any action
        return false;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * <b>In this implementation, does nothing.</b>
     */
    @Override
    public boolean removeAll(ObjectSet<O> objectSet, QueryOptions queryOptions) {
        // No need to take any action
        return false;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * <b>In this implementation, stores a reference to the supplied collection, which the
     * {@link Index#retrieve(com.googlecode.cqengine.query.Query, com.googlecode.cqengine.query.option.QueryOptions)} method can subsequently iterate.</b>
     */
    @Override
    public void init(ObjectStore<O> objectStore, QueryOptions queryOptions) {
        // Store the collection...
        this.objectStore = objectStore;
    }

    /**
     * This is a no-op for this type of index.
     * @param queryOptions Optional parameters for the update
     */
    @Override
    public void destroy(QueryOptions queryOptions) {
        // No-op
    }

    /**
     * {@inheritDoc}
     * @param queryOptions
     */
    @Override
    public void clear(QueryOptions queryOptions) {
        objectStore.clear(queryOptions);
    }
}
