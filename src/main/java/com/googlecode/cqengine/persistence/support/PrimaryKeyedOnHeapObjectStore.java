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

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.index.support.CloseableIterator;
import com.googlecode.cqengine.query.option.QueryOptions;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * An on-heap {@link ObjectStore} which uses the primary key as identity and iteration order.
 *
 * @author niall.gallagher
 */
public class PrimaryKeyedOnHeapObjectStore<O, A extends Comparable<A>> implements PrimaryKeyedObjectStore<O> {

    final SimpleAttribute<O, A> primaryKeyAttribute;
    final ConcurrentNavigableMap<A, O> backingMap = new ConcurrentSkipListMap<A, O>();
    final PrimaryKeyedOnHeapObjectStoreIndex<O, A> backingIndex;

    public PrimaryKeyedOnHeapObjectStore(SimpleAttribute<O, A> primaryKeyAttribute) {
        this.primaryKeyAttribute = primaryKeyAttribute;
        this.backingIndex = new PrimaryKeyedOnHeapObjectStoreIndex<O, A>(this);
    }

    @Override
    public SimpleAttribute<O, A> getPrimaryKeyAttribute() {
        return primaryKeyAttribute;
    }

    public PrimaryKeyedOnHeapObjectStoreIndex<O, A> getBackingIndex() {
        return backingIndex;
    }

    @Override
    public Object getPrimaryKeyForObject(Object object, QueryOptions queryOptions) {
        return extractPrimaryKeyOrNull(object, queryOptions);
    }

    @Override
    public O getObjectByPrimaryKey(Object object, QueryOptions queryOptions) {
        final A primaryKey = extractPrimaryKeyOrNull(object, queryOptions);
        return primaryKey == null ? null : backingMap.get(primaryKey);
    }

    @Override
    public int size(QueryOptions queryOptions) {
        return backingMap.size();
    }

    @Override
    public boolean contains(Object o, QueryOptions queryOptions) {
        final A primaryKey = extractPrimaryKeyOrNull(o, queryOptions);
        return primaryKey != null && backingMap.containsKey(primaryKey);
    }

    @Override
    public CloseableIterator<O> iterator(QueryOptions queryOptions) {
        final Iterator<Map.Entry<A, O>> backingIterator = backingMap.entrySet().iterator();
        return new CloseableIterator<O>() {
            Map.Entry<A, O> currentEntry = null;

            @Override
            public void close() {
                // No op
            }

            @Override
            public boolean hasNext() {
                return backingIterator.hasNext();
            }

            @Override
            public O next() {
                currentEntry = backingIterator.next();
                return currentEntry.getValue();
            }

            @Override
            public void remove() {
                if (currentEntry == null) {
                    throw new IllegalStateException();
                }
                backingMap.remove(currentEntry.getKey(), currentEntry.getValue());
                currentEntry = null;
            }
        };
    }

    @Override
    public boolean isEmpty(QueryOptions queryOptions) {
        return backingMap.isEmpty();
    }

    @Override
    public boolean add(O object, QueryOptions queryOptions) {
        return addOrReplace(object, queryOptions).isModified();
    }

    @Override
    public ModificationResult<O> addOrReplace(O object, QueryOptions queryOptions) {
        final A primaryKey = extractPrimaryKey(object, queryOptions);
        while (true) {
            final O existingObject = backingMap.putIfAbsent(primaryKey, object);
            if (existingObject == null) {
                return ModificationResult.inserted(object);
            }
            if (existingObject == object) {
                return ModificationResult.unchanged(existingObject);
            }
            if (backingMap.replace(primaryKey, existingObject, object)) {
                return ModificationResult.replaced(existingObject, object);
            }
        }
    }

    @Override
    public boolean remove(Object o, QueryOptions queryOptions) {
        return removeByPrimaryKey(o, queryOptions).isModified();
    }

    @Override
    public ModificationResult<O> removeByPrimaryKey(Object object, QueryOptions queryOptions) {
        final A primaryKey = extractPrimaryKeyOrNull(object, queryOptions);
        if (primaryKey == null) {
            return ModificationResult.notFound();
        }
        final O removedObject = backingMap.remove(primaryKey);
        return removedObject == null ? ModificationResult.notFound() : ModificationResult.removed(removedObject);
    }

    @Override
    public boolean containsAll(Collection<?> c, QueryOptions queryOptions) {
        for (Object candidate : c) {
            if (!contains(candidate, queryOptions)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends O> c, QueryOptions queryOptions) {
        boolean modified = false;
        for (O object : c) {
            modified = addOrReplace(object, queryOptions).isModified() || modified;
        }
        return modified;
    }

    @Override
    public boolean retainAll(Collection<?> c, QueryOptions queryOptions) {
        final Set<A> primaryKeysToRetain = new HashSet<A>(c.size());
        for (Object candidate : c) {
            final A primaryKey = extractPrimaryKeyOrNull(candidate, queryOptions);
            if (primaryKey != null) {
                primaryKeysToRetain.add(primaryKey);
            }
        }

        final Collection<A> primaryKeysToRemove = new ArrayList<A>();
        for (Map.Entry<A, O> entry : backingMap.entrySet()) {
            if (!primaryKeysToRetain.contains(entry.getKey())) {
                primaryKeysToRemove.add(entry.getKey());
            }
        }

        boolean modified = false;
        for (A primaryKey : primaryKeysToRemove) {
            modified = backingMap.remove(primaryKey) != null || modified;
        }
        return modified;
    }

    @Override
    public boolean removeAll(Collection<?> c, QueryOptions queryOptions) {
        boolean modified = false;
        for (Object object : c) {
            modified = removeByPrimaryKey(object, queryOptions).isModified() || modified;
        }
        return modified;
    }

    @Override
    public void clear(QueryOptions queryOptions) {
        backingMap.clear();
    }

    A extractPrimaryKey(O object, QueryOptions queryOptions) {
        if (object == null) {
            throw new NullPointerException("Object was null");
        }
        final A primaryKey = primaryKeyAttribute.getValue(object, queryOptions);
        if (primaryKey == null) {
            throw new IllegalStateException("Primary key attribute returned null for object: " + object);
        }
        return primaryKey;
    }

    A extractPrimaryKeyOrNull(Object object, QueryOptions queryOptions) {
        if (object == null) {
            return null;
        }
        try {
            @SuppressWarnings("unchecked")
            final O typedObject = (O) object;
            return extractPrimaryKey(typedObject, queryOptions);
        }
        catch (ClassCastException e) {
            return null;
        }
    }
}
