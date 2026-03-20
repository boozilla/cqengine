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
package com.googlecode.cqengine.indexingbenchmark;

import com.googlecode.cqengine.testutil.Car;
import com.googlecode.cqengine.testutil.CarFactory;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexingBenchmarkUnitTest {

    private final Collection<Car> collection = CarFactory.createCollectionOfCars(1000);

    @Test
    public void testIndexingBenchmarkRunnerCollectsTimingsForRegisteredTasks() {
        assertFalse(IndexingBenchmarkRunner.benchmarkTasks.isEmpty());

        for (final IndexingTask task : IndexingBenchmarkRunner.benchmarkTasks) {
            final IndexingTaskTimings timings = IndexingBenchmarkRunner.runBenchmarkTask(task, 1, collection);

            assertEquals(task.getClass().getSimpleName(), timings.testName);
            assertEquals(Integer.valueOf(collection.size()), timings.collectionSize);
            assertTrue(timings.timeTakenPerCollection >= 0L);
            assertTrue(timings.timeTakenPerObject >= 0L);
        }
    }
}
