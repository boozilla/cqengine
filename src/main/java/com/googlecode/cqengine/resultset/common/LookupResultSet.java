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

import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.resultset.ResultSet;

import java.util.Collections;
import java.util.Iterator;

/**
 * A {@link ResultSet} which resolves its backing result set lazily on every method call.
 */
public abstract class LookupResultSet<O> extends ResultSet<O> {

    final Query<O> query;
    final QueryOptions queryOptions;
    final int retrievalCost;

    protected LookupResultSet(Query<O> query, QueryOptions queryOptions, int retrievalCost) {
        this.query = query;
        this.queryOptions = queryOptions;
        this.retrievalCost = retrievalCost;
    }

    protected abstract ResultSet<O> lookupResultSet();

    @Override
    public Iterator<O> iterator() {
        final ResultSet<O> resultSet = lookupResultSet();
        return resultSet == null ? Collections.<O>emptyList().iterator() : resultSet.iterator();
    }

    @Override
    public boolean contains(O object) {
        final ResultSet<O> resultSet = lookupResultSet();
        return resultSet != null && resultSet.contains(object);
    }

    @Override
    public boolean matches(O object) {
        return query.matches(object, queryOptions);
    }

    @Override
    public Query<O> getQuery() {
        return query;
    }

    @Override
    public QueryOptions getQueryOptions() {
        return queryOptions;
    }

    @Override
    public int getRetrievalCost() {
        return retrievalCost;
    }

    @Override
    public int getMergeCost() {
        final ResultSet<O> resultSet = lookupResultSet();
        return resultSet == null ? 0 : resultSet.size();
    }

    @Override
    public int size() {
        final ResultSet<O> resultSet = lookupResultSet();
        return resultSet == null ? 0 : resultSet.size();
    }

    @Override
    public void close() {
        // No op.
    }
}
