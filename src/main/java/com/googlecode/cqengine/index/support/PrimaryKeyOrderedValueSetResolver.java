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
package com.googlecode.cqengine.index.support;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.persistence.Persistence;
import com.googlecode.cqengine.persistence.onheap.OnHeapPersistence;
import com.googlecode.cqengine.query.option.QueryOptions;

/**
 * Resolves the collection primary key attribute once for indexes whose per-key value sets can preserve primary order.
 */
public class PrimaryKeyOrderedValueSetResolver<O> {

    private volatile boolean resolved = false;
    private volatile SimpleAttribute<O, ? extends Comparable> primaryKeyAttribute = null;

    @SuppressWarnings("unchecked")
    public SimpleAttribute<O, ? extends Comparable> getPrimaryKeyAttribute(QueryOptions queryOptions) {
        if (resolved) {
            return primaryKeyAttribute;
        }
        final Persistence<O, ? extends Comparable> persistence = (Persistence<O, ? extends Comparable>) queryOptions.get(Persistence.class);
        if (persistence == null) {
            return null;
        }
        synchronized (this) {
            if (!resolved) {
                primaryKeyAttribute = persistence instanceof OnHeapPersistence ? persistence.getPrimaryKeyAttribute() : null;
                resolved = true;
            }
        }
        return primaryKeyAttribute;
    }
}
