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
package com.googlecode.cqengine.resultset.iterator;

import com.googlecode.concurrenttrees.common.LazyIterator;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.index.support.KeyValue;
import com.googlecode.cqengine.index.support.KeyValueMaterialized;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.resultset.filter.MaterializedDeduplicatedIterator;
import com.googlecode.cqengine.resultset.order.AttributeOrdersComparator;

import java.util.*;

/**
 * @author Niall Gallagher
 */
public class IteratorUtil {

    static final int DIRECT_SIMPLE_ATTRIBUTE_SORT_MAX_SIZE = 128;

    public static <O> boolean iterableContains(Iterable<O> iterable, O element) {
        for (O contained : iterable) {
            if (contained.equals(element)) {
                return true;
            }
        }
        return false;
    }

    public static int countElements(Iterable<?> iterable) {
        int count = 0;
        // UnusedDeclaration warning is due to iterator.next() being invoked but not used.
        // Actually we intentionally invoke iterator.next() even though we don't use it,
        // in case iterator implementation requires this per typical usage...
        //noinspection UnusedDeclaration
        for (Object object : iterable) {
            count++;
        }
        return count;
    }

    /**
     * Returns the elements of {@code unfiltered} that are not null.
     */
    public static <T> Iterator<T> removeNulls(final Iterator<T> unfiltered) {
        return new LazyIterator<T>() {
            @Override protected T computeNext() {
                while (unfiltered.hasNext()) {
                    T element = unfiltered.next();
                    if (element != null) {
                        return element;
                    }
                }
                return endOfData();
            }
        };
    }

    /**
     * Wraps the given Iterator as an {@link UnmodifiableIterator}.
     */
    public static <T> UnmodifiableIterator<T> wrapAsUnmodifiable(final Iterator<T> iterator) {
        return new UnmodifiableIterator<T>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return iterator.next();
            }
        };
    }

    /**
     * Transforms a {@code Map&lt;A, Iterable&lt;O&gt;&gt;} into a stream of {@code KeyValue&lt;A, O&gt;} objects.
     *
     * @param map The map to be transformed
     * @param <A> Type of the key in the map
     * @param <O> Type of the objects returned by the Iterables in the map
     * @return A flattened stream of {@code KeyValue&lt;A, O&gt;} objects
     */
    public static <A, O> Iterable<KeyValue<A, O>> flatten(final Map<A, ? extends Iterable<O>> map) {
        return new Iterable<KeyValue<A, O>>() {
            @Override
            public Iterator<KeyValue<A, O>> iterator() {
                return new LazyIterator<KeyValue<A, O>>() {
                    final Iterator<? extends Map.Entry<A, ? extends Iterable<O>>> entriesIterator = map.entrySet().iterator();
                    Iterator<KeyValue<A, O>> valuesIterator = Collections.emptyIterator();
                    @Override
                    protected KeyValue<A, O> computeNext() {
                        while (true) {
                            if (valuesIterator.hasNext()) {
                                return valuesIterator.next();
                            }
                            if (!entriesIterator.hasNext()) {
                                return endOfData();
                            }
                            Map.Entry<A, ? extends Iterable<O>> entry = entriesIterator.next();
                            valuesIterator = flatten(entry.getKey(), entry.getValue()).iterator();
                        }
                    }
                };
            }
        };
    }

    /**
     * Transforms a key of type {@code A} and an {@code Iterable&lt;O&gt;} into a stream of {@code KeyValue&lt;A, O&gt;}
     * objects.
     *
     * @param key The key to be transformed
     * @param values The values to be transformed
     * @param <A> Type of the key
     * @param <O> Type of the objects returned by the Iterable
     * @return A flattened stream of {@code KeyValue&lt;A, O&gt;} objects
     */
    public static <A, O> Iterable<KeyValue<A, O>> flatten(final A key, final Iterable<O> values) {
        return new Iterable<KeyValue<A, O>>() {
            @Override
            public Iterator<KeyValue<A, O>> iterator() {
                return new LazyIterator<KeyValue<A, O>>() {
                    final Iterator<O> valuesIterator = values.iterator();
                    @Override
                    protected KeyValue<A, O> computeNext() {
                        return valuesIterator.hasNext() ? new KeyValueMaterialized<A, O>(key, valuesIterator.next()) : endOfData();
                    }
                };
            }
        };
    }

    public static <O> Iterator<O> concatenate(final Iterator<? extends Iterable<O>> iterables) {
        return new ConcatenatingIterator<O>() {
            @Override
            public Iterator<O> getNextIterator() {
                return iterables.hasNext() ? iterables.next().iterator() : null;
            }
        };
    }

    public static <O> Iterator<Set<O>> groupAndSort(final Iterator<? extends KeyValue<?, O>> values, final Comparator<O> comparator) {
        return new LazyIterator<Set<O>>() {
            final Iterator<? extends KeyValue<?, O>> valuesIterator = values;
            List<O> currentGroup = null;
            Object currentKey = null;
            boolean groupStarted = false;

            @Override
            protected Set<O> computeNext() {

                while (valuesIterator.hasNext()) {
                    final KeyValue<?, O> next = valuesIterator.next();
                    if (!groupStarted) {
                        currentKey = next.getKey();
                        currentGroup = new ArrayList<O>(16);
                        currentGroup.add(next.getValue());
                        groupStarted = true;
                        continue;
                    }
                    if (!next.getKey().equals(currentKey)) {
                        final Set<O> result = sortedGroup(currentGroup, comparator);
                        currentKey = next.getKey();
                        currentGroup = new ArrayList<O>(16);
                        currentGroup.add(next.getValue());
                        return result;
                    }
                    currentGroup.add(next.getValue());
                }
                if (!groupStarted) {
                    return endOfData();
                }
                final Set<O> result = sortedGroup(currentGroup, comparator);
                currentGroup = null;
                currentKey = null;
                groupStarted = false;
                return result;
            }
        };
    }

    static <O> Set<O> sortedGroup(List<O> currentGroup, Comparator<O> comparator) {
        if (currentGroup.isEmpty()) {
            return Collections.emptySet();
        }
        if (currentGroup.size() > 1) {
            if (comparator instanceof AttributeOrdersComparator) {
                currentGroup = ((AttributeOrdersComparator<O>) comparator).sortAndDeduplicate(currentGroup);
            }
            else {
                currentGroup.sort(comparator);
                currentGroup = deduplicateSortedGroup(currentGroup, comparator);
            }
        }
        return new ComparatorBackedSet<O>(currentGroup, comparator);
    }

    static <O> List<O> deduplicateSortedGroup(List<O> sortedGroup, Comparator<O> comparator) {
        O previous = sortedGroup.get(0);
        for (int i = 1; i < sortedGroup.size(); i++) {
            final O current = sortedGroup.get(i);
            if (comparator.compare(previous, current) == 0) {
                final List<O> deduplicated = new ArrayList<O>(sortedGroup.size());
                deduplicated.addAll(sortedGroup.subList(0, i));
                for (int j = i + 1; j < sortedGroup.size(); j++) {
                    final O candidate = sortedGroup.get(j);
                    if (comparator.compare(previous, candidate) != 0) {
                        deduplicated.add(candidate);
                        previous = candidate;
                    }
                }
                return deduplicated;
            }
            previous = current;
        }
        return sortedGroup;
    }

    static class ComparatorBackedSet<O> extends AbstractSet<O> {
        final List<O> values;
        final Comparator<O> comparator;

        ComparatorBackedSet(List<O> values, Comparator<O> comparator) {
            this.values = values;
            this.comparator = comparator;
        }

        @Override
        public Iterator<O> iterator() {
            return wrapAsUnmodifiable(values.iterator());
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean contains(Object o) {
            try {
                return Collections.binarySearch(values, (O) o, comparator) >= 0;
            }
            catch (ClassCastException e) {
                return false;
            }
        }
    }

    /**
     * Sorts the results returned by the given iterator, returning the sorted results as a new iterator, by performing
     * a merge-sort into an intermediate array in memory.
     * <p/>
     * The time complexity for copying the objects into the intermediate array is O(n), and then the cost of sorting is
     * additionally O(n log(n)). So overall complexity is O(n) + O(n log(n)).
     * <p>
     * Note this method does not perform any deduplication of objects. It can be combined with
     * {@link #materializedDeduplicate(Iterator)} to achieve that.
     *
     * @param unsortedIterator An iterator which provides unsorted objects
     * @param comparator The comparator to use for sorting
     * @param <O> The type of the objects to be sorted
     * @return An iterator which returns the objects in sorted order
     */
    public static <O> Iterator<O> materializedSort(Iterator<O> unsortedIterator, Comparator<O> comparator) {
        final List<O> result = new ArrayList<O>();
        while (unsortedIterator.hasNext()) {
            result.add(unsortedIterator.next());
        }
        if (comparator instanceof AttributeOrdersComparator) {
            ((AttributeOrdersComparator<O>) comparator).sort(result);
        }
        else {
            result.sort(comparator);
        }
        return result.iterator();
    }

    public static <O, A extends Comparable> Iterator<O> materializedSortBySimpleAttribute(Iterator<O> unsortedIterator, SimpleAttribute<O, A> attribute, QueryOptions queryOptions, boolean descending) {
        final List<O> values = new ArrayList<O>();
        while (unsortedIterator.hasNext()) {
            values.add(unsortedIterator.next());
        }
        if (values.size() <= DIRECT_SIMPLE_ATTRIBUTE_SORT_MAX_SIZE) {
            values.sort(simpleAttributeComparator(attribute, queryOptions, descending));
            return values.iterator();
        }

        final List<AttributeKeyValue<O, A>> result = new ArrayList<AttributeKeyValue<O, A>>(values.size());
        for (final O object : values) {
            result.add(new AttributeKeyValue<O, A>(attribute.getValue(object, queryOptions), object));
        }
        result.sort(descending ? DESCENDING_SIMPLE_ATTRIBUTE_ORDER : ASCENDING_SIMPLE_ATTRIBUTE_ORDER);
        return new Iterator<O>() {
            final Iterator<AttributeKeyValue<O, A>> iterator = result.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public O next() {
                return iterator.next().value;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    static <O, A extends Comparable> Comparator<O> simpleAttributeComparator(final SimpleAttribute<O, A> attribute, final QueryOptions queryOptions, final boolean descending) {
        return new Comparator<O>() {
            @Override
            public int compare(O left, O right) {
                final int comparison = compareComparableValues(attribute.getValue(left, queryOptions), attribute.getValue(right, queryOptions));
                return descending ? comparison * -1 : comparison;
            }
        };
    }

    /**
     * De-duplicates the results returned by the given iterator, by wrapping it in a
     * {@link MaterializedDeduplicatedIterator}.
     */
    public static <O> Iterator<O> materializedDeduplicate(Iterator<O> iterator) {
        return new MaterializedDeduplicatedIterator<O>(iterator);
    }

    static final Comparator<AttributeKeyValue<?, ? extends Comparable>> ASCENDING_SIMPLE_ATTRIBUTE_ORDER = new Comparator<AttributeKeyValue<?, ? extends Comparable>>() {
        @Override
        public int compare(AttributeKeyValue<?, ? extends Comparable> left, AttributeKeyValue<?, ? extends Comparable> right) {
            return compareComparableValues(left.key, right.key);
        }
    };

    static final Comparator<AttributeKeyValue<?, ? extends Comparable>> DESCENDING_SIMPLE_ATTRIBUTE_ORDER = new Comparator<AttributeKeyValue<?, ? extends Comparable>>() {
        @Override
        public int compare(AttributeKeyValue<?, ? extends Comparable> left, AttributeKeyValue<?, ? extends Comparable> right) {
            return compareComparableValues(right.key, left.key);
        }
    };

    @SuppressWarnings({"rawtypes", "unchecked"})
    static int compareComparableValues(Comparable left, Comparable right) {
        return left.compareTo(right);
    }

    static class AttributeKeyValue<O, A extends Comparable> {
        final A key;
        final O value;

        AttributeKeyValue(A key, O value) {
            this.key = key;
            this.value = value;
        }
    }
}
