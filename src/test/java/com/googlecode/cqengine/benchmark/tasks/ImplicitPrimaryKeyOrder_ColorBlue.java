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
package com.googlecode.cqengine.benchmark.tasks;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.benchmark.BenchmarkTask;
import com.googlecode.cqengine.index.navigable.NavigableIndex;
import com.googlecode.cqengine.persistence.onheap.OnHeapPersistence;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.resultset.ResultSet;
import com.googlecode.cqengine.testutil.Car;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static com.googlecode.cqengine.query.QueryFactory.equal;

/**
 * Benchmarks the implicit primary-key ordering path against a selective secondary filter.
 */
public class ImplicitPrimaryKeyOrder_ColorBlue implements BenchmarkTask {

    private Collection<Car> collection;
    private IndexedCollection<Car> indexedCollection;

    private final Query<Car> query = equal(Car.COLOR, Car.Color.BLUE);

    @Override
    public void init(Collection<Car> collection) {
        this.collection = collection;
        final IndexedCollection<Car> indexedCollection1 = new ConcurrentIndexedCollection<Car>(OnHeapPersistence.onPrimaryKey(Car.CAR_ID));
        indexedCollection1.addIndex(NavigableIndex.onAttribute(Car.COLOR));
        indexedCollection1.addAll(collection);
        this.indexedCollection = indexedCollection1;
    }

    @Override
    public int runQueryCountResults_IterationNaive() {
        final List<Car> results = new LinkedList<Car>();
        for (final Car car : collection) {
            if (car.getColor() == Car.Color.BLUE) {
                results.add(car);
            }
        }
        return BenchmarkTaskUtil.countResultsViaIteration(results);
    }

    @Override
    public int runQueryCountResults_IterationOptimized() {
        int count = 0;
        for (final Car car : collection) {
            if (car.getColor() == Car.Color.BLUE) {
                count++;
            }
        }
        return count;
    }

    @Override
    public int runQueryCountResults_CQEngine() {
        final ResultSet<Car> results = indexedCollection.retrieve(query);
        return BenchmarkTaskUtil.countResultsViaIteration(results);
    }

    @Override
    public int runQueryCountResults_CQEngineStatistics() {
        final ResultSet<Car> results = indexedCollection.retrieve(query);
        return results.size();
    }
}
