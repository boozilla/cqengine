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
package com.googlecode.cqengine.resultset.order;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.resultset.ResultSet;
import com.googlecode.cqengine.resultset.common.WrappedResultSet;
import com.googlecode.cqengine.resultset.iterator.IteratorUtil;

import java.util.Iterator;

/**
 * A {@link ResultSet} variant which materializes and sorts by a single {@link SimpleAttribute}.
 * Stable sorting preserves the input order for equal keys, which allows callers to reuse an existing
 * deterministic suffix order when appropriate.
 */
public class MaterializedSimpleAttributeOrderedResultSet<O, A extends Comparable> extends WrappedResultSet<O> {

    final SimpleAttribute<O, A> attribute;
    final QueryOptions queryOptions;
    final boolean descending;
    final boolean preserveWrappedCosts;

    public MaterializedSimpleAttributeOrderedResultSet(ResultSet<O> wrappedResultSet, SimpleAttribute<O, A> attribute, QueryOptions queryOptions, boolean descending, boolean preserveWrappedCosts) {
        super(wrappedResultSet);
        this.attribute = attribute;
        this.queryOptions = queryOptions;
        this.descending = descending;
        this.preserveWrappedCosts = preserveWrappedCosts;
    }

    @Override
    public Iterator<O> iterator() {
        return IteratorUtil.materializedSortBySimpleAttribute(super.iterator(), attribute, queryOptions, descending);
    }

    @Override
    public int getMergeCost() {
        if (preserveWrappedCosts) {
            return wrappedResultSet.getMergeCost();
        }
        long mergeCost = wrappedResultSet.getMergeCost();
        mergeCost = (2 * mergeCost) + (mergeCost * (long)Math.log(mergeCost));
        mergeCost = mergeCost < 0 ? Long.MAX_VALUE : mergeCost;
        return (int)Math.min(mergeCost, Integer.MAX_VALUE);
    }

    @Override
    public int size() {
        return preserveWrappedCosts ? wrappedResultSet.size() : IteratorUtil.countElements(this);
    }

    @Override
    public boolean isEmpty() {
        return wrappedResultSet.isEmpty();
    }

    @Override
    public boolean isNotEmpty() {
        return wrappedResultSet.isNotEmpty();
    }
}
