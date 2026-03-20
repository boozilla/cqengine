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
package com.googlecode.cqengine.benchmark;

import com.googlecode.cqengine.benchmark.tasks.*;
import com.googlecode.cqengine.testutil.Car;
import com.googlecode.cqengine.testutil.CarFactory;
import org.junit.Test;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Tests for the benchmark itself.
 *
 * @author Niall Gallagher
 */
public class BenchmarkUnitTest {

    private final Collection<Car> collection = CarFactory.createCollectionOfCars(1000);

    @Test
    public void testHashIndex_ModelFocus() {
        BenchmarkTask task = new HashIndex_ModelFocus();
        task.init(collection);

        assertEquals(100, task.runQueryCountResults_IterationNaive());
        assertEquals(100, task.runQueryCountResults_IterationOptimized());
        assertEquals(100, task.runQueryCountResults_CQEngine());
        assertEquals(100, task.runQueryCountResults_CQEngineStatistics());
    }

    @Test
    public void testHashIndex_ManufacturerFord() {
        BenchmarkTask task = new HashIndex_ManufacturerFord();
        task.init(collection);

        assertEquals(300, task.runQueryCountResults_IterationNaive());
        assertEquals(300, task.runQueryCountResults_IterationOptimized());
        assertEquals(300, task.runQueryCountResults_CQEngine());
        assertEquals(300, task.runQueryCountResults_CQEngineStatistics());
    }

    @Test
    public void testNavigableIndex_PriceBetween() {
        BenchmarkTask task = new NavigableIndex_PriceBetween();
        task.init(collection);

        assertEquals(200, task.runQueryCountResults_IterationNaive());
        assertEquals(200, task.runQueryCountResults_IterationOptimized());
        assertEquals(200, task.runQueryCountResults_CQEngine());
        assertEquals(200, task.runQueryCountResults_CQEngineStatistics());
    }

    @Test
    public void testCompoundIndex_ManufacturerToyotaColorBlueDoorsThree() {
        BenchmarkTask task = new CompoundIndex_ManufacturerToyotaColorBlueDoorsThree();
        task.init(collection);

        assertEquals(100, task.runQueryCountResults_IterationNaive());
        assertEquals(100, task.runQueryCountResults_IterationOptimized());
        assertEquals(100, task.runQueryCountResults_CQEngine());
        assertEquals(100, task.runQueryCountResults_CQEngineStatistics());
    }

    @Test
    public void testStandingQueryIndex_ManufacturerToyotaColorBlueDoorsNotFive() {
        BenchmarkTask task = new StandingQueryIndex_ManufacturerToyotaColorBlueDoorsNotFive();
        task.init(collection);

        assertEquals(100, task.runQueryCountResults_IterationNaive());
        assertEquals(100, task.runQueryCountResults_IterationOptimized());
        assertEquals(100, task.runQueryCountResults_CQEngine());
        assertEquals(100, task.runQueryCountResults_CQEngineStatistics());
    }

    @Test
    public void testRadixTreeIndex_ModelStartsWithF() {
        BenchmarkTask task = new RadixTreeIndex_ModelStartsWithP();
        task.init(collection);

        assertEquals(100, task.runQueryCountResults_IterationNaive());
        assertEquals(100, task.runQueryCountResults_IterationOptimized());
        assertEquals(100, task.runQueryCountResults_CQEngine());
        assertEquals(100, task.runQueryCountResults_CQEngineStatistics());
    }

    @Test
    public void testSuffixTreeIndex_ModelContainsI() {
        BenchmarkTask task = new SuffixTreeIndex_ModelContainsG();
        task.init(collection);

        assertEquals(100, task.runQueryCountResults_IterationNaive());
        assertEquals(100, task.runQueryCountResults_IterationOptimized());
        assertEquals(100, task.runQueryCountResults_CQEngine());
        assertEquals(100, task.runQueryCountResults_CQEngineStatistics());
    }

    @Test
    public void testHashIndex_CarId() {
        BenchmarkTask task = new HashIndex_CarId();
        task.init(collection);

        assertEquals(1, task.runQueryCountResults_IterationNaive());
        assertEquals(1, task.runQueryCountResults_IterationOptimized());
        assertEquals(1, task.runQueryCountResults_CQEngine());
        assertEquals(1, task.runQueryCountResults_CQEngineStatistics());
    }

    @Test
    public void testUniqueIndex_CarId() {
        BenchmarkTask task = new UniqueIndex_CarId();
        task.init(collection);

        assertEquals(1, task.runQueryCountResults_IterationNaive());
        assertEquals(1, task.runQueryCountResults_IterationOptimized());
        assertEquals(1, task.runQueryCountResults_CQEngine());
        assertEquals(1, task.runQueryCountResults_CQEngineStatistics());
    }

    @Test
    public void testQuantized_HashIndex_CarId() {
        BenchmarkTask task = new Quantized_HashIndex_CarId();
        task.init(collection);

        assertEquals(1, task.runQueryCountResults_IterationNaive());
        assertEquals(1, task.runQueryCountResults_IterationOptimized());
        assertEquals(1, task.runQueryCountResults_CQEngine());
        assertEquals(1, task.runQueryCountResults_CQEngineStatistics());
    }

    @Test
    public void testQuantized_NavigableIndex_CarId() {
        BenchmarkTask task = new Quantized_NavigableIndex_CarId();
        task.init(collection);

        assertEquals(3, task.runQueryCountResults_IterationNaive());
        assertEquals(3, task.runQueryCountResults_IterationOptimized());
        assertEquals(3, task.runQueryCountResults_CQEngine());
        assertEquals(3, task.runQueryCountResults_CQEngineStatistics());
    }

    @Test
    public void testNonOptimalIndexes() {
        BenchmarkTask task = new NonOptimalIndexes_ManufacturerToyotaColorBlueDoorsThree();
        task.init(collection);

        assertEquals(100, task.runQueryCountResults_IterationNaive());
        assertEquals(100, task.runQueryCountResults_IterationOptimized());
        assertEquals(100, task.runQueryCountResults_CQEngine());
        assertEquals(100, task.runQueryCountResults_CQEngineStatistics());
    }

    @Test
    public void testNoIndexes_ModelFocus() {
        BenchmarkTask task = new NoIndexes_ModelFocus();
        task.init(collection);

        assertEquals(100, task.runQueryCountResults_IterationNaive());
        assertEquals(100, task.runQueryCountResults_IterationOptimized());
        assertEquals(100, task.runQueryCountResults_CQEngine());
        assertEquals(100, task.runQueryCountResults_CQEngineStatistics());
    }

    @Test
    public void testImplicitPrimaryKeyOrder_ColorBlue() {
        BenchmarkTask task = new ImplicitPrimaryKeyOrder_ColorBlue();
        task.init(collection);

        assertEquals(200, task.runQueryCountResults_IterationNaive());
        assertEquals(200, task.runQueryCountResults_IterationOptimized());
        assertEquals(200, task.runQueryCountResults_CQEngine());
        assertEquals(200, task.runQueryCountResults_CQEngineStatistics());
    }

    @Test
    public void testMaterializedOrder_CardId() {
        BenchmarkTask task = new MaterializedOrder_CardId();
        task.init(collection);

        assertEquals(100, task.runQueryCountResults_IterationNaive());
        assertEquals(100, task.runQueryCountResults_IterationOptimized());
        assertEquals(100, task.runQueryCountResults_CQEngine());
        assertEquals(100, task.runQueryCountResults_CQEngineStatistics());
    }

    @Test
    public void testRegisteredBenchmarkTasksReturnExpectedCounts() {
        final Map<Class<? extends BenchmarkTask>, Integer> expectedCounts = expectedCountsByTask();

        assertFalse(BenchmarkRunner.benchmarkTasks.isEmpty());
        assertEquals(expectedCounts.size(), BenchmarkRunner.benchmarkTasks.size());

        for (final BenchmarkTask task : BenchmarkRunner.benchmarkTasks) {
            final Integer expectedCount = expectedCounts.get(task.getClass());

            assertNotNull(task.getClass().getName(), expectedCount);

            task.init(collection);

            assertEquals(expectedCount.intValue(), task.runQueryCountResults_IterationNaive());
            assertEquals(expectedCount.intValue(), task.runQueryCountResults_IterationOptimized());
            assertEquals(expectedCount.intValue(), task.runQueryCountResults_CQEngine());
            assertEquals(expectedCount.intValue(), task.runQueryCountResults_CQEngineStatistics());
        }
    }

    @Test
    public void testBenchmarkRunnerCollectsTimingsForRegisteredTasks() {
        assertFalse(BenchmarkRunner.benchmarkTasks.isEmpty());

        for (final BenchmarkTask task : BenchmarkRunner.benchmarkTasks) {
            task.init(collection);

            final BenchmarkTaskTimings timings = BenchmarkRunner.runBenchmarkTask(task, 1);

            assertEquals(task.getClass().getSimpleName(), timings.testName);
            assertTrue(timings.timeTakenIterationNaive >= 0L);
            assertTrue(timings.timeTakenIterationOptimized >= 0L);
            assertTrue(timings.timeTakenCQEngine >= 0L);
            assertTrue(timings.timeTakenCQEngineStatistics >= 0L);
        }
    }

    private static Map<Class<? extends BenchmarkTask>, Integer> expectedCountsByTask() {
        final Map<Class<? extends BenchmarkTask>, Integer> expectedCounts = new LinkedHashMap<Class<? extends BenchmarkTask>, Integer>();
        expectedCounts.put(UniqueIndex_CarId.class, 1);
        expectedCounts.put(HashIndex_CarId.class, 1);
        expectedCounts.put(HashIndex_ManufacturerFord.class, 300);
        expectedCounts.put(HashIndex_ModelFocus.class, 100);
        expectedCounts.put(NavigableIndex_PriceBetween.class, 200);
        expectedCounts.put(Quantized_HashIndex_CarId.class, 1);
        expectedCounts.put(Quantized_NavigableIndex_CarId.class, 3);
        expectedCounts.put(CompoundIndex_ManufacturerToyotaColorBlueDoorsThree.class, 100);
        expectedCounts.put(NoIndexes_ModelFocus.class, 100);
        expectedCounts.put(NonOptimalIndexes_ManufacturerToyotaColorBlueDoorsThree.class, 100);
        expectedCounts.put(ImplicitPrimaryKeyOrder_ColorBlue.class, 200);
        expectedCounts.put(StandingQueryIndex_ManufacturerToyotaColorBlueDoorsNotFive.class, 100);
        expectedCounts.put(RadixTreeIndex_ModelStartsWithP.class, 100);
        expectedCounts.put(SuffixTreeIndex_ModelContainsG.class, 100);
        expectedCounts.put(MaterializedOrder_CardId.class, 100);
        return expectedCounts;
    }
}
