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

import com.googlecode.cqengine.index.support.CloseableIterator;
import com.googlecode.cqengine.query.option.QueryOptions;

import java.util.Collection;

/**
 * Wraps an {@link ObjectStore} and exposes a read-only view of it.
 *
 * @author niall.gallagher
 */
public class ReadOnlyObjectStore<O> implements ObjectStore<O> {

    final ObjectStore<O> backingObjectStore;

    public ReadOnlyObjectStore(ObjectStore<O> backingObjectStore) {
        this.backingObjectStore = backingObjectStore;
    }

    public ObjectStore<O> getBackingObjectStore() {
        return backingObjectStore;
    }

    @SuppressWarnings("unchecked")
    public static <O> ObjectStore<O> unwrap(ObjectStore<O> objectStore) {
        while (objectStore instanceof ReadOnlyObjectStore) {
            objectStore = ((ReadOnlyObjectStore<O>) objectStore).getBackingObjectStore();
        }
        return objectStore;
    }

    @Override
    public int size(QueryOptions queryOptions) {
        return backingObjectStore.size(queryOptions);
    }

    @Override
    public boolean contains(Object o, QueryOptions queryOptions) {
        return backingObjectStore.contains(o, queryOptions);
    }

    @Override
    public CloseableIterator<O> iterator(QueryOptions queryOptions) {
        final CloseableIterator<O> backingIterator = backingObjectStore.iterator(queryOptions);
        return new CloseableIterator<O>() {
            @Override
            public void close() {
                backingIterator.close();
            }

            @Override
            public boolean hasNext() {
                return backingIterator.hasNext();
            }

            @Override
            public O next() {
                return backingIterator.next();
            }

            @Override
            public void remove() {
                throw modificationNotSupported();
            }
        };
    }

    @Override
    public boolean isEmpty(QueryOptions queryOptions) {
        return backingObjectStore.isEmpty(queryOptions);
    }

    @Override
    public boolean add(O object, QueryOptions queryOptions) {
        throw modificationNotSupported();
    }

    @Override
    public boolean remove(Object o, QueryOptions queryOptions) {
        throw modificationNotSupported();
    }

    @Override
    public boolean containsAll(Collection<?> c, QueryOptions queryOptions) {
        return backingObjectStore.containsAll(c, queryOptions);
    }

    @Override
    public boolean addAll(Collection<? extends O> c, QueryOptions queryOptions) {
        throw modificationNotSupported();
    }

    @Override
    public boolean retainAll(Collection<?> c, QueryOptions queryOptions) {
        throw modificationNotSupported();
    }

    @Override
    public boolean removeAll(Collection<?> c, QueryOptions queryOptions) {
        throw modificationNotSupported();
    }

    @Override
    public void clear(QueryOptions queryOptions) {
        throw modificationNotSupported();
    }

    protected UnsupportedOperationException modificationNotSupported() {
        return new UnsupportedOperationException("Modification not supported on a read-only object store");
    }
}
