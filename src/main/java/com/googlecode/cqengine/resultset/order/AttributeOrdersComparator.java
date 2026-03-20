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

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.OrderControlAttribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.AttributeOrder;
import com.googlecode.cqengine.query.option.QueryOptions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * A comparator which sorts result objects according to a list of attributes each with an associated preference for
 * ascending or descending order.
 *
 * @author Roberto Socrates
 * @author Niall Gallagher
 */
public class AttributeOrdersComparator<O> implements Comparator<O> {

    final List<AttributeOrder<O>> attributeSortOrders;
    final List<SortInstruction<O>> sortInstructions;
    final QueryOptions queryOptions;
    final boolean supportsPreExtractedSortKeys;
    final boolean supportsSingleExtractedSortKey;
    final boolean useTieBreaker;

    public AttributeOrdersComparator(List<AttributeOrder<O>> attributeSortOrders, QueryOptions queryOptions) {
        this(attributeSortOrders, queryOptions, true);
    }

    public AttributeOrdersComparator(List<AttributeOrder<O>> attributeSortOrders, QueryOptions queryOptions, boolean useTieBreaker) {
        this.attributeSortOrders = attributeSortOrders;
        this.queryOptions = queryOptions;
        this.useTieBreaker = useTieBreaker;
        this.sortInstructions = new ArrayList<SortInstruction<O>>(attributeSortOrders.size());
        boolean supportsPreExtractedSortKeys = true;
        for (AttributeOrder<O> attributeSortOrder : attributeSortOrders) {
            final SortInstruction<O> sortInstruction = new SortInstruction<O>(attributeSortOrder);
            sortInstructions.add(sortInstruction);
            supportsPreExtractedSortKeys &= sortInstruction.supportsPreExtractedSortKeys();
        }
        this.supportsPreExtractedSortKeys = supportsPreExtractedSortKeys;
        this.supportsSingleExtractedSortKey = sortInstructions.size() == 1 && supportsPreExtractedSortKeys && sortInstructions.get(0).orderControl == null;
    }

    @Override
    public int compare(O o1, O o2) {
        for (SortInstruction<O> sortInstruction : sortInstructions) {
            final int comparison = sortInstruction.compare(o1, o2, queryOptions);
            if (comparison != 0) {
                // Found a difference. Invert the sign if order is descending, and return it...
                return sortInstruction.descending ? comparison * -1 : comparison;
            }
            // else continue checking remaining attributes.
        }
        // No differences found according to ordering specified, but in case this comparator
        // will be used for object equality testing, return 0 only if objects really are equal...
        if (o1.equals(o2)) {
            return 0;
        }

        // At this point we have run out of attributeSortOrders: they all returned 0, but because the objects are not
        // equal, we cannot return 0.

        // Example: This might occur when sorting by [color, price] and two or more different items have the same color
        // and the same price. Because a third - tie-breaking - sort order was not supplied, the sort order of items
        // which have the same color and price, is unspecified.

        // However although the sort order is now unspecified, we should try to preserve the stability of the comparison
        // as much as possible: such that
        //                              if comparator.compare(o1, o2) == -1, then it should be the case that
        //                                 comparator.compare(o2, o1) == +1.
        // Thus we cannot simply return the same result in both of those cases. This can be important when objects are
        // added to collections such as {@link java.util.TreeSet} which depend on the stability of the comparison.

        // As we don't have any other readily available properties of the objects to use as tie-breakers, we will use
        // the hashcodes of the objects instead. As the hashcodes of unequal objects are unlikely to be the same *most
        // of the time*, using the hashcodes as a tie-breaker will result in a stable comparison *most of the time*.
        // In the rare event that the hashcodes of the unequal objects turn out to be the same however, we will return
        // 1 because it seems more important that the comparator is consistent with equals() than it is to be stable...
        return useTieBreaker ? compareTieBreak(o1, o2) : 0;
    }

    public void sort(List<O> values) {
        if (values.size() < 2) {
            return;
        }
        if (supportsSingleExtractedSortKey) {
            sortSingleAttribute(values);
            return;
        }
        if (!supportsPreExtractedSortKeys) {
            values.sort(this);
            return;
        }
        final List<ExtractedSortKey<O>> extractedSortKeys = extractSortKeys(values);
        extractedSortKeys.sort(this::compareExtractedSortKeys);
        for (int i = 0; i < extractedSortKeys.size(); i++) {
            values.set(i, extractedSortKeys.get(i).object);
        }
    }

    public List<O> sortAndDeduplicate(List<O> values) {
        if (values.size() < 2) {
            return values;
        }
        if (supportsSingleExtractedSortKey) {
            return sortAndDeduplicateSingleAttribute(values);
        }
        if (!supportsPreExtractedSortKeys) {
            values.sort(this);
            return values;
        }
        final List<ExtractedSortKey<O>> extractedSortKeys = extractSortKeys(values);
        extractedSortKeys.sort(this::compareExtractedSortKeys);

        ExtractedSortKey<O> previous = extractedSortKeys.get(0);
        for (int i = 1; i < extractedSortKeys.size(); i++) {
            final ExtractedSortKey<O> current = extractedSortKeys.get(i);
            if (compareExtractedSortKeys(previous, current) == 0) {
                final List<O> deduplicated = new ArrayList<O>(extractedSortKeys.size());
                deduplicated.add(previous.object);
                for (int j = i; j < extractedSortKeys.size(); j++) {
                    final ExtractedSortKey<O> candidate = extractedSortKeys.get(j);
                    if (compareExtractedSortKeys(previous, candidate) != 0) {
                        deduplicated.add(candidate.object);
                        previous = candidate;
                    }
                }
                return deduplicated;
            }
            previous = current;
        }
        for (int i = 0; i < extractedSortKeys.size(); i++) {
            values.set(i, extractedSortKeys.get(i).object);
        }
        return values;
    }

    void sortSingleAttribute(List<O> values) {
        final List<SingleExtractedSortKey<O>> extractedSortKeys = extractSingleSortKeys(values);
        extractedSortKeys.sort(this::compareSingleExtractedSortKeys);
        for (int i = 0; i < extractedSortKeys.size(); i++) {
            values.set(i, extractedSortKeys.get(i).object);
        }
    }

    List<O> sortAndDeduplicateSingleAttribute(List<O> values) {
        final List<SingleExtractedSortKey<O>> extractedSortKeys = extractSingleSortKeys(values);
        extractedSortKeys.sort(this::compareSingleExtractedSortKeys);

        SingleExtractedSortKey<O> previous = extractedSortKeys.get(0);
        for (int i = 1; i < extractedSortKeys.size(); i++) {
            final SingleExtractedSortKey<O> current = extractedSortKeys.get(i);
            if (compareSingleExtractedSortKeys(previous, current) == 0) {
                final List<O> deduplicated = new ArrayList<O>(extractedSortKeys.size());
                deduplicated.add(previous.object);
                for (int j = i; j < extractedSortKeys.size(); j++) {
                    final SingleExtractedSortKey<O> candidate = extractedSortKeys.get(j);
                    if (compareSingleExtractedSortKeys(previous, candidate) != 0) {
                        deduplicated.add(candidate.object);
                        previous = candidate;
                    }
                }
                return deduplicated;
            }
            previous = current;
        }
        for (int i = 0; i < extractedSortKeys.size(); i++) {
            values.set(i, extractedSortKeys.get(i).object);
        }
        return values;
    }

    List<SingleExtractedSortKey<O>> extractSingleSortKeys(List<O> values) {
        final SortInstruction<O> sortInstruction = sortInstructions.get(0);
        final List<SingleExtractedSortKey<O>> extractedSortKeys = new ArrayList<SingleExtractedSortKey<O>>(values.size());
        for (O value : values) {
            extractedSortKeys.add(new SingleExtractedSortKey<O>(value, sortInstruction, queryOptions));
        }
        return extractedSortKeys;
    }

    List<ExtractedSortKey<O>> extractSortKeys(List<O> values) {
        final List<ExtractedSortKey<O>> extractedSortKeys = new ArrayList<ExtractedSortKey<O>>(values.size());
        for (O value : values) {
            extractedSortKeys.add(new ExtractedSortKey<O>(value, sortInstructions, queryOptions));
        }
        return extractedSortKeys;
    }

    int compareExtractedSortKeys(ExtractedSortKey<O> left, ExtractedSortKey<O> right) {
        for (int i = 0; i < sortInstructions.size(); i++) {
            final SortInstruction<O> sortInstruction = sortInstructions.get(i);
            if (sortInstruction.orderControl != null) {
                final int comparison = compareComparableValues(left.orderControlValues[i], right.orderControlValues[i]);
                if (comparison != 0) {
                    return sortInstruction.descending ? comparison * -1 : comparison;
                }
            }
            final int comparison = compareComparableValues(left.attributeValues[i], right.attributeValues[i]);
            if (comparison != 0) {
                return sortInstruction.descending ? comparison * -1 : comparison;
            }
        }
        return useTieBreaker ? compareTieBreak(left.object, right.object) : 0;
    }

    int compareSingleExtractedSortKeys(SingleExtractedSortKey<O> left, SingleExtractedSortKey<O> right) {
        final SortInstruction<O> sortInstruction = sortInstructions.get(0);
        final int comparison = compareComparableValues(left.attributeValue, right.attributeValue);
        if (comparison != 0) {
            return sortInstruction.descending ? comparison * -1 : comparison;
        }
        return useTieBreaker ? compareTieBreak(left.object, right.object) : 0;
    }

    static int compareTieBreak(Object o1, Object o2) {
        if (o1.equals(o2)) {
            return 0;
        }
        return o1.hashCode() >= o2.hashCode() ? 1 : -1;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static int compareComparableValues(Comparable o1, Comparable o2) {
        return o1.compareTo(o2);
    }

    static <O> int compareAttributeValues(Attribute<O, ? extends Comparable> attribute, O o1, O o2, QueryOptions queryOptions) {
        if (attribute instanceof SimpleAttribute) {
            // Fast code path...
            final SimpleAttribute<O, ? extends Comparable> simpleAttribute = (SimpleAttribute<O, ? extends Comparable>)attribute;
            return compareComparableValues(simpleAttribute.getValue(o1, queryOptions), simpleAttribute.getValue(o2, queryOptions));
        }
        else {
            // Slower code path...
            final Iterator<? extends Comparable> o1attributeValues = attribute.getValues(o1, queryOptions).iterator();
            final Iterator<? extends Comparable> o2attributeValues = attribute.getValues(o2, queryOptions).iterator();
            while (o1attributeValues.hasNext() && o2attributeValues.hasNext()) {
                final Comparable o1attributeValue = o1attributeValues.next();
                final Comparable o2attributeValue = o2attributeValues.next();
                final int comparison = compareComparableValues(o1attributeValue, o2attributeValue);
                if (comparison != 0) {
                    // If we found a difference, return it...
                    return comparison;
                }
            }
            // If the number of attribute values differs, return a difference, object with fewest elements first...
            if (o2attributeValues.hasNext()) {
                return -1;
            }
            else if (o1attributeValues.hasNext()) {
                return +1;
            }
            // No differences found...
            return 0;
        }
    }

    static class SortInstruction<O> {
        final boolean descending;
        final OrderControlAttribute<O> orderControl;
        final Attribute<O, ? extends Comparable> attribute;
        final SimpleAttribute<O, ? extends Comparable> simpleAttribute;

        @SuppressWarnings({"rawtypes", "unchecked"})
        SortInstruction(AttributeOrder<O> attributeOrder) {
            this.descending = attributeOrder.isDescending();
            final Attribute<O, ? extends Comparable> attribute = attributeOrder.getAttribute();
            if (attribute instanceof OrderControlAttribute) {
                this.orderControl = (OrderControlAttribute<O>) (OrderControlAttribute) attribute;
                this.attribute = (Attribute<O, ? extends Comparable>) this.orderControl.getDelegateAttribute();
            }
            else {
                this.orderControl = null;
                this.attribute = (Attribute<O, Comparable>) attribute;
            }
            this.simpleAttribute = this.attribute instanceof SimpleAttribute
                    ? (SimpleAttribute<O, ? extends Comparable>) this.attribute
                    : null;
        }

        boolean supportsPreExtractedSortKeys() {
            return simpleAttribute != null;
        }

        int compare(O o1, O o2, QueryOptions queryOptions) {
            if (orderControl != null) {
                final int comparison = compareComparableValues(orderControl.getValue(o1, queryOptions), orderControl.getValue(o2, queryOptions));
                if (comparison != 0) {
                    // One of the objects has values for the delegate attribute encapsulated in OrderControlAttribute,
                    // and the other object does not. Return this difference so that they will be ordered relative to
                    // each other based whether they have values or not...
                    return comparison;
                }
            }
            if (simpleAttribute != null) {
                return compareComparableValues(simpleAttribute.getValue(o1, queryOptions), simpleAttribute.getValue(o2, queryOptions));
            }
            return compareAttributeValues(attribute, o1, o2, queryOptions);
        }
    }

    static class ExtractedSortKey<O> {
        final O object;
        final Comparable[] orderControlValues;
        final Comparable[] attributeValues;

        ExtractedSortKey(O object, List<SortInstruction<O>> sortInstructions, QueryOptions queryOptions) {
            this.object = object;
            this.orderControlValues = new Comparable[sortInstructions.size()];
            this.attributeValues = new Comparable[sortInstructions.size()];
            for (int i = 0; i < sortInstructions.size(); i++) {
                final SortInstruction<O> sortInstruction = sortInstructions.get(i);
                if (sortInstruction.orderControl != null) {
                    orderControlValues[i] = sortInstruction.orderControl.getValue(object, queryOptions);
                }
                attributeValues[i] = sortInstruction.simpleAttribute.getValue(object, queryOptions);
            }
        }
    }

    static class SingleExtractedSortKey<O> {
        final O object;
        final Comparable attributeValue;

        SingleExtractedSortKey(O object, SortInstruction<O> sortInstruction, QueryOptions queryOptions) {
            this.object = object;
            this.attributeValue = sortInstruction.simpleAttribute.getValue(object, queryOptions);
        }
    }
}
