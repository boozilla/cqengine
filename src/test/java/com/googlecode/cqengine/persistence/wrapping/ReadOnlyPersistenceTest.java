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

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.Index;
import com.googlecode.cqengine.index.navigable.NavigableIndex;
import com.googlecode.cqengine.persistence.onheap.OnHeapPersistence;
import com.googlecode.cqengine.persistence.support.ObjectStore;
import com.googlecode.cqengine.persistence.support.PrimaryKeyedOnHeapObjectStore;
import com.googlecode.cqengine.persistence.support.PrimaryKeyedOnHeapObjectStoreIndex;
import com.googlecode.cqengine.resultset.ResultSet;
import com.googlecode.cqengine.testutil.Car;
import com.googlecode.cqengine.testutil.CarFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import static com.googlecode.cqengine.query.QueryFactory.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * Tests for {@link ReadOnlyPersistence}.
 *
 * @author npgall
 */
public class ReadOnlyPersistenceTest {

    @Test
    public void testWrappingPersistence_BlocksMutationsButAllowsQueries() {
        final Collection<Car> backingCollection = new LinkedHashSet<Car>();
        backingCollection.addAll(CarFactory.createCollectionOfCars(3)); // CarIds 0, 1, 2

        final IndexedCollection<Car> indexedCollection = new ConcurrentIndexedCollection<Car>(
                ReadOnlyPersistence.around(WrappingPersistence.aroundCollection(backingCollection))
        );
        indexedCollection.addIndex(NavigableIndex.onAttribute(Car.CAR_ID));

        final ResultSet<Car> results = indexedCollection.retrieve(greaterThan(Car.CAR_ID, 0));
        try {
            assertNotEquals(Integer.MAX_VALUE, results.getRetrievalCost());
            assertEquals(asList(1, 2), carIds(results));
        }
        finally {
            results.close();
        }

        expectUnsupportedOperation(new Runnable() {
            @Override
            public void run() {
                indexedCollection.add(CarFactory.createCar(3));
            }
        });
        expectUnsupportedOperation(new Runnable() {
            @Override
            public void run() {
                indexedCollection.remove(CarFactory.createCar(0));
            }
        });
        expectUnsupportedOperation(new Runnable() {
            @Override
            public void run() {
                indexedCollection.update(asList(CarFactory.createCar(0)), asList(CarFactory.createCar(3)));
            }
        });
        expectUnsupportedOperation(new Runnable() {
            @Override
            public void run() {
                indexedCollection.clear();
            }
        });
        expectUnsupportedIteratorRemove(indexedCollection);

        assertEquals(asList(0, 1, 2), carIds(backingCollection));
    }

    @Test
    public void testPrimaryKeyedReadOnlyPersistence_RetainsBackingIndexAndOrdering() {
        final IndexedCollection<Car> indexedCollection = new ConcurrentIndexedCollection<Car>(
                ReadOnlyPersistence.around(new PrePopulatedPrimaryKeyedOnHeapPersistence())
        );

        final List<Index<Car>> indexes = new ArrayList<Index<Car>>();
        for (Index<Car> index : indexedCollection.getIndexes()) {
            indexes.add(index);
        }
        assertEquals(1, indexes.size());
        assertTrue(indexes.get(0) instanceof PrimaryKeyedOnHeapObjectStoreIndex);

        assertEquals(asList(0, 1, 2), carIds(indexedCollection));

        final ResultSet<Car> results = indexedCollection.retrieve(has(Car.CAR_ID));
        try {
            assertEquals(asList(0, 1, 2), carIds(results));
        }
        finally {
            results.close();
        }

        expectUnsupportedOperation(new Runnable() {
            @Override
            public void run() {
                indexedCollection.add(CarFactory.createCar(3));
            }
        });
    }

    static void expectUnsupportedOperation(Runnable runnable) {
        try {
            runnable.run();
            fail("UnsupportedOperationException expected");
        }
        catch (UnsupportedOperationException expected) {
            // expected
        }
    }

    static void expectUnsupportedIteratorRemove(IndexedCollection<Car> indexedCollection) {
        final java.util.Iterator<Car> iterator = indexedCollection.iterator();
        iterator.next();
        try {
            iterator.remove();
            fail("UnsupportedOperationException expected");
        }
        catch (UnsupportedOperationException expected) {
            // expected
        }
        finally {
            if (iterator instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) iterator).close();
                }
                catch (Exception e) {
                    throw new IllegalStateException("Failed to close iterator", e);
                }
            }
        }
    }

    static List<Integer> carIds(Iterable<Car> cars) {
        final List<Integer> carIds = new ArrayList<Integer>();
        for (Car car : cars) {
            carIds.add(car.getCarId());
        }
        return carIds;
    }

    static class PrePopulatedPrimaryKeyedOnHeapPersistence extends OnHeapPersistence<Car, Integer> {

        PrePopulatedPrimaryKeyedOnHeapPersistence() {
            super(Car.CAR_ID);
        }

        @Override
        public ObjectStore<Car> createObjectStore() {
            final PrimaryKeyedOnHeapObjectStore<Car, Integer> objectStore = new PrimaryKeyedOnHeapObjectStore<Car, Integer>(Car.CAR_ID);
            objectStore.addAll(asList(CarFactory.createCar(2), CarFactory.createCar(0), CarFactory.createCar(1)), noQueryOptions());
            return objectStore;
        }
    }
}
