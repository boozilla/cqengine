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
package com.googlecode.cqengine.resultset.stored;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.resultset.order.PrimaryKeyOrderedResultSet;

import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A stored result set backed by a primary-key-ordered map.
 */
public class PrimaryKeyOrderedStoredResultSet<O, A extends Comparable> extends StoredResultSet<O> implements PrimaryKeyOrderedResultSet<O> {

    final SimpleAttribute<O, A> primaryKeyAttribute;
    final ConcurrentNavigableMap<A, O> backingMap = new ConcurrentSkipListMap<A, O>();
    final int retrievalCost;

    public PrimaryKeyOrderedStoredResultSet(SimpleAttribute<O, A> primaryKeyAttribute) {
        this(primaryKeyAttribute, 0);
    }

    public PrimaryKeyOrderedStoredResultSet(SimpleAttribute<O, A> primaryKeyAttribute, int retrievalCost) {
        this.primaryKeyAttribute = primaryKeyAttribute;
        this.retrievalCost = retrievalCost;
    }

    @Override
    public SimpleAttribute<O, ? extends Comparable> getPrimaryKeyAttribute() {
        return primaryKeyAttribute;
    }

    @Override
    public boolean contains(O object) {
        final A primaryKey = extractPrimaryKeyOrNull(object);
        return primaryKey != null && backingMap.containsKey(primaryKey);
    }

    @Override
    public boolean matches(O object) {
        return contains(object);
    }

    @Override
    public Iterator<O> iterator() {
        return backingMap.values().iterator();
    }

    @Override
    public boolean add(O object) {
        final A primaryKey = extractPrimaryKey(object);
        return backingMap.putIfAbsent(primaryKey, object) == null;
    }

    @Override
    public boolean remove(O object) {
        final A primaryKey = extractPrimaryKeyOrNull(object);
        return primaryKey != null && backingMap.remove(primaryKey) != null;
    }

    @Override
    public boolean isEmpty() {
        return backingMap.isEmpty();
    }

    @Override
    public boolean isNotEmpty() {
        return !backingMap.isEmpty();
    }

    @Override
    public void clear() {
        backingMap.clear();
    }

    @Override
    public int size() {
        return backingMap.size();
    }

    @Override
    public int getRetrievalCost() {
        return retrievalCost;
    }

    @Override
    public int getMergeCost() {
        return backingMap.size();
    }

    @Override
    public void close() {
        // No op.
    }

    @Override
    public Query<O> getQuery() {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryOptions getQueryOptions() {
        throw new UnsupportedOperationException();
    }

    A extractPrimaryKey(O object) {
        if (object == null) {
            throw new NullPointerException("Object was null");
        }
        final A primaryKey = primaryKeyAttribute.getValue(object, null);
        if (primaryKey == null) {
            throw new IllegalStateException("Primary key attribute returned null for object: " + object);
        }
        return primaryKey;
    }

    A extractPrimaryKeyOrNull(Object object) {
        if (object == null) {
            return null;
        }
        try {
            @SuppressWarnings("unchecked")
            final O typedObject = (O) object;
            return extractPrimaryKey(typedObject);
        }
        catch (ClassCastException e) {
            return null;
        }
    }
}
