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

import com.googlecode.cqengine.resultset.ResultSet;

import java.util.Comparator;

/**
 * A {@link MaterializedOrderedResultSet} variant used for implicit default ordering.
 * It preserves the wrapped result-set cost metrics so that default presentation ordering
 * does not change query-planning or externally visible cost expectations.
 *
 * @author niall.gallagher
 */
public class CostPreservingMaterializedOrderedResultSet<O> extends MaterializedOrderedResultSet<O> {

    public CostPreservingMaterializedOrderedResultSet(ResultSet<O> wrappedResultSet, Comparator<O> comparator) {
        super(wrappedResultSet, comparator);
    }

    @Override
    public int getMergeCost() {
        return wrappedResultSet.getMergeCost();
    }

    @Override
    public int size() {
        return wrappedResultSet.size();
    }
}
