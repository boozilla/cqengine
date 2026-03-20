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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Parses optional command line arguments shared by benchmark runners.
 */
public class BenchmarkRunnerOptions {

    private final List<String> taskFilters;
    private final int warmupRepetitions;
    private final int measurementRepetitions;

    BenchmarkRunnerOptions(List<String> taskFilters, int warmupRepetitions, int measurementRepetitions) {
        this.taskFilters = Collections.unmodifiableList(new ArrayList<String>(taskFilters));
        this.warmupRepetitions = warmupRepetitions;
        this.measurementRepetitions = measurementRepetitions;
    }

    public static BenchmarkRunnerOptions parse(String[] args, int defaultWarmupRepetitions, int defaultMeasurementRepetitions) {
        final List<String> taskFilters = new ArrayList<String>();
        int warmupRepetitions = defaultWarmupRepetitions;
        int measurementRepetitions = defaultMeasurementRepetitions;

        for (int i = 0; i < args.length; i++) {
            final String argument = args[i];
            if ("--task".equals(argument)) {
                addTaskFilters(taskFilters, requireValue(argument, args, ++i));
            }
            else if (argument.startsWith("--task=")) {
                addTaskFilters(taskFilters, argument.substring("--task=".length()));
            }
            else if ("--warmup".equals(argument)) {
                warmupRepetitions = parseRepetitions(argument, requireValue(argument, args, ++i));
            }
            else if (argument.startsWith("--warmup=")) {
                warmupRepetitions = parseRepetitions("--warmup", argument.substring("--warmup=".length()));
            }
            else if ("--measurement".equals(argument) || "--measure".equals(argument)) {
                measurementRepetitions = parseRepetitions(argument, requireValue(argument, args, ++i));
            }
            else if (argument.startsWith("--measurement=")) {
                measurementRepetitions = parseRepetitions("--measurement", argument.substring("--measurement=".length()));
            }
            else if (argument.startsWith("--measure=")) {
                measurementRepetitions = parseRepetitions("--measure", argument.substring("--measure=".length()));
            }
            else {
                addTaskFilters(taskFilters, argument);
            }
        }

        return new BenchmarkRunnerOptions(taskFilters, warmupRepetitions, measurementRepetitions);
    }

    static String requireValue(String optionName, String[] args, int valueIndex) {
        if (valueIndex >= args.length) {
            throw new IllegalArgumentException("Missing value for option: " + optionName);
        }
        return args[valueIndex];
    }

    static void addTaskFilters(List<String> taskFilters, String rawValue) {
        final String[] filters = rawValue.split(",");
        for (final String filter : filters) {
            final String trimmedFilter = filter.trim();
            if (!trimmedFilter.isEmpty()) {
                taskFilters.add(trimmedFilter);
            }
        }
    }

    static int parseRepetitions(String optionName, String rawValue) {
        try {
            final int repetitions = Integer.parseInt(rawValue);
            if (repetitions < 0) {
                throw new IllegalArgumentException("Option must be zero or greater: " + optionName);
            }
            return repetitions;
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Failed to parse integer value for option " + optionName + ": " + rawValue, e);
        }
    }

    public boolean matchesTaskName(String taskName) {
        if (taskFilters.isEmpty()) {
            return true;
        }
        final String lowerCaseTaskName = taskName.toLowerCase(Locale.ENGLISH);
        for (final String taskFilter : taskFilters) {
            if (lowerCaseTaskName.contains(taskFilter.toLowerCase(Locale.ENGLISH))) {
                return true;
            }
        }
        return false;
    }

    public List<String> getTaskFilters() {
        return taskFilters;
    }

    public int getWarmupRepetitions() {
        return warmupRepetitions;
    }

    public int getMeasurementRepetitions() {
        return measurementRepetitions;
    }
}
