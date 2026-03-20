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
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.resultset.order.PrimaryKeyOrderedResultSet;

/**
 * A {@link LookupResultSet} whose iterator order is already ascending by the collection primary key.
 */
public abstract class PrimaryKeyOrderedLookupResultSet<O> extends LookupResultSet<O> implements PrimaryKeyOrderedResultSet<O> {

    final SimpleAttribute<O, ? extends Comparable> primaryKeyAttribute;

    protected PrimaryKeyOrderedLookupResultSet(Query<O> query, QueryOptions queryOptions, int retrievalCost, SimpleAttribute<O, ? extends Comparable> primaryKeyAttribute) {
        super(query, queryOptions, retrievalCost);
        this.primaryKeyAttribute = primaryKeyAttribute;
    }

    @Override
    public SimpleAttribute<O, ? extends Comparable> getPrimaryKeyAttribute() {
        return primaryKeyAttribute;
    }
}
