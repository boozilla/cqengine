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
import com.googlecode.cqengine.query.option.QueryOptions;

/**
 * A read-only view over a {@link PrimaryKeyedObjectStore}.
 *
 * @author niall.gallagher
 */
public class ReadOnlyPrimaryKeyedObjectStore<O> extends ReadOnlyObjectStore<O> implements PrimaryKeyedObjectStore<O> {

    final PrimaryKeyedObjectStore<O> backingPrimaryKeyedObjectStore;

    public ReadOnlyPrimaryKeyedObjectStore(PrimaryKeyedObjectStore<O> backingPrimaryKeyedObjectStore) {
        super(backingPrimaryKeyedObjectStore);
        this.backingPrimaryKeyedObjectStore = backingPrimaryKeyedObjectStore;
    }

    @Override
    public SimpleAttribute<O, ? extends Comparable> getPrimaryKeyAttribute() {
        return backingPrimaryKeyedObjectStore.getPrimaryKeyAttribute();
    }

    @Override
    public Object getPrimaryKeyForObject(Object object, QueryOptions queryOptions) {
        return backingPrimaryKeyedObjectStore.getPrimaryKeyForObject(object, queryOptions);
    }

    @Override
    public O getObjectByPrimaryKey(Object object, QueryOptions queryOptions) {
        return backingPrimaryKeyedObjectStore.getObjectByPrimaryKey(object, queryOptions);
    }

    @Override
    public ModificationResult<O> addOrReplace(O object, QueryOptions queryOptions) {
        throw modificationNotSupported();
    }

    @Override
    public ModificationResult<O> removeByPrimaryKey(Object object, QueryOptions queryOptions) {
        throw modificationNotSupported();
    }
}
