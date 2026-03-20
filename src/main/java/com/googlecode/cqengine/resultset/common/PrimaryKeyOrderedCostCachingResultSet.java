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
package com.googlecode.cqengine.resultset.common;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.resultset.ResultSet;
import com.googlecode.cqengine.resultset.order.PrimaryKeyOrderedResultSet;

/**
 * A {@link CostCachingResultSet} which preserves primary-key-order metadata from the wrapped result set.
 */
public class PrimaryKeyOrderedCostCachingResultSet<O> extends CostCachingResultSet<O> implements PrimaryKeyOrderedResultSet<O> {

    final PrimaryKeyOrderedResultSet<O> wrappedPrimaryKeyOrderedResultSet;

    @SuppressWarnings("unchecked")
    public PrimaryKeyOrderedCostCachingResultSet(ResultSet<O> wrappedResultSet) {
        super(wrappedResultSet);
        this.wrappedPrimaryKeyOrderedResultSet = (PrimaryKeyOrderedResultSet<O>) wrappedResultSet;
    }

    @Override
    public SimpleAttribute<O, ? extends Comparable> getPrimaryKeyAttribute() {
        return wrappedPrimaryKeyOrderedResultSet.getPrimaryKeyAttribute();
    }
}
