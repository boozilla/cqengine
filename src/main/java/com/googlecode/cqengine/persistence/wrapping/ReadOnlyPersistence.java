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
package com.googlecode.cqengine.persistence.wrapping;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.index.Index;
import com.googlecode.cqengine.persistence.Persistence;
import com.googlecode.cqengine.persistence.support.ObjectStore;
import com.googlecode.cqengine.persistence.support.PrimaryKeyedObjectStore;
import com.googlecode.cqengine.persistence.support.ReadOnlyObjectStore;
import com.googlecode.cqengine.persistence.support.ReadOnlyPrimaryKeyedObjectStore;
import com.googlecode.cqengine.query.option.QueryOptions;

/**
 * Wraps an existing {@link Persistence} and exposes its object store as read-only.
 *
 * @author niall.gallagher
 */
public class ReadOnlyPersistence<O, A extends Comparable<A>> implements Persistence<O, A> {

    final Persistence<O, A> backingPersistence;

    public ReadOnlyPersistence(Persistence<O, A> backingPersistence) {
        this.backingPersistence = backingPersistence;
    }

    public Persistence<O, A> getBackingPersistence() {
        return backingPersistence;
    }

    @SuppressWarnings("unchecked")
    public static <O, A extends Comparable<A>> Persistence<O, A> unwrap(Persistence<O, A> persistence) {
        while (persistence instanceof ReadOnlyPersistence) {
            persistence = ((ReadOnlyPersistence<O, A>) persistence).getBackingPersistence();
        }
        return persistence;
    }

    @Override
    public ObjectStore<O> createObjectStore() {
        final ObjectStore<O> objectStore = backingPersistence.createObjectStore();
        if (objectStore instanceof PrimaryKeyedObjectStore) {
            @SuppressWarnings("unchecked")
            final PrimaryKeyedObjectStore<O> primaryKeyedObjectStore = (PrimaryKeyedObjectStore<O>) objectStore;
            return new ReadOnlyPrimaryKeyedObjectStore<O>(primaryKeyedObjectStore);
        }
        return new ReadOnlyObjectStore<O>(objectStore);
    }

    @Override
    public boolean supportsIndex(Index<O> index) {
        return backingPersistence.supportsIndex(index);
    }

    @Override
    public void openRequestScopeResources(QueryOptions queryOptions) {
        backingPersistence.openRequestScopeResources(queryOptions);
    }

    @Override
    public void closeRequestScopeResources(QueryOptions queryOptions) {
        backingPersistence.closeRequestScopeResources(queryOptions);
    }

    @Override
    public SimpleAttribute<O, A> getPrimaryKeyAttribute() {
        return backingPersistence.getPrimaryKeyAttribute();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ReadOnlyPersistence)) {
            return false;
        }

        ReadOnlyPersistence<?, ?> that = (ReadOnlyPersistence<?, ?>) o;

        return backingPersistence.equals(that.backingPersistence);
    }

    @Override
    public int hashCode() {
        return backingPersistence.hashCode();
    }

    @Override
    public String toString() {
        return "ReadOnlyPersistence{" +
                "backingPersistence=" + backingPersistence +
                '}';
    }

    public static <O, A extends Comparable<A>> ReadOnlyPersistence<O, A> around(Persistence<O, A> persistence) {
        return new ReadOnlyPersistence<O, A>(persistence);
    }
}
