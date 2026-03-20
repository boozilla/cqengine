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
package com.googlecode.cqengine.persistence.support.sqlite;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.index.sqlite.SQLiteIdentityIndex;
import com.googlecode.cqengine.index.sqlite.SQLitePersistence;
import com.googlecode.cqengine.index.support.CloseableIterator;
import com.googlecode.cqengine.persistence.support.ObjectSet;
import com.googlecode.cqengine.persistence.support.ObjectStore;
import com.googlecode.cqengine.persistence.support.PrimaryKeyedObjectStore;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.resultset.ResultSet;
import com.googlecode.cqengine.resultset.iterator.UnmodifiableIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.Map;

import static com.googlecode.cqengine.query.QueryFactory.equal;
import static com.googlecode.cqengine.query.QueryFactory.has;

/**
 * @author niall.gallagher
 */
public class SQLiteObjectStore<O, A extends Comparable<A>> implements ObjectStore<O>, PrimaryKeyedObjectStore<O> {

    final SQLitePersistence<O, A> persistence;
    final SQLiteIdentityIndex<A, O> backingIndex;
    final SimpleAttribute<O, A> primaryKeyAttribute;
    final Class<O> objectType;

    public SQLiteObjectStore(final SQLitePersistence<O, A> persistence) {
        this.persistence = persistence;
        this.objectType = persistence.getPrimaryKeyAttribute().getObjectType();
        this.primaryKeyAttribute = persistence.getPrimaryKeyAttribute();
        this.backingIndex = persistence.createIdentityIndex();
    }

    public void init(QueryOptions queryOptions) {
        backingIndex.init(this, queryOptions);
    }

    public SQLitePersistence<O, A> getPersistence() {
        return persistence;
    }

    public SQLiteIdentityIndex<A, O> getBackingIndex() {
        return backingIndex;
    }

    @Override
    public SimpleAttribute<O, A> getPrimaryKeyAttribute() {
        return primaryKeyAttribute;
    }

    @Override
    public Object getPrimaryKeyForObject(Object object, QueryOptions queryOptions) {
        return extractPrimaryKeyOrNull(object, queryOptions);
    }

    @Override
    public O getObjectByPrimaryKey(Object object, QueryOptions queryOptions) {
        final A objectId = extractPrimaryKeyOrNull(object, queryOptions);
        if (objectId == null) {
            return null;
        }
        final ResultSet<O> results = backingIndex.retrieve(equal(primaryKeyAttribute, objectId), queryOptions);
        try {
            final Iterator<O> iterator = results.iterator();
            return iterator.hasNext() ? iterator.next() : null;
        }
        finally {
            results.close();
        }
    }

    @Override
    public int size(QueryOptions queryOptions) {
        return backingIndex.retrieve(has(primaryKeyAttribute), queryOptions).size();
    }

    @Override
    public boolean contains(Object o, QueryOptions queryOptions) {
        @SuppressWarnings("unchecked")
        O object = (O) o;
        A objectId = primaryKeyAttribute.getValue(object, queryOptions);
        return backingIndex.retrieve(equal(primaryKeyAttribute, objectId), queryOptions).size() > 0;
    }

    @Override
    public CloseableIterator<O> iterator(QueryOptions queryOptions) {
        final ResultSet<O> rs = backingIndex.retrieve(has(primaryKeyAttribute), queryOptions);
        final Iterator<O> i = rs.iterator();
        class CloseableIteratorImpl extends UnmodifiableIterator<O> implements CloseableIterator<O> {

            @Override
            public boolean hasNext() {
                return i.hasNext();
            }

            @Override
            public O next() {
                return i.next();
            }

            @Override
            public void close() {
                rs.close();
            }
        }
        return new CloseableIteratorImpl();
    }

    @Override
    public boolean isEmpty(QueryOptions queryOptions) {
        return size(queryOptions) == 0;
    }

    @Override
    public boolean add(O object, QueryOptions queryOptions) {
        return addOrReplace(object, queryOptions).isModified();
    }

    @Override
    public ModificationResult<O> addOrReplace(O object, QueryOptions queryOptions) {
        final O existingObject = getObjectByPrimaryKey(object, queryOptions);
        if (existingObject != null && existingObject.equals(object)) {
            return ModificationResult.unchanged(existingObject);
        }
        if (existingObject != null) {
            removeByPrimaryKey(object, queryOptions);
        }
        final boolean modified = backingIndex.addAll(ObjectSet.fromCollection(Collections.singleton(object)), queryOptions);
        if (!modified) {
            return existingObject == null ? ModificationResult.<O>notFound() : ModificationResult.unchanged(existingObject);
        }
        return existingObject == null ? ModificationResult.inserted(object) : ModificationResult.replaced(existingObject, object);
    }

    @Override
    public boolean remove(Object o, QueryOptions queryOptions) {
        return removeByPrimaryKey(o, queryOptions).isModified();
    }

    @Override
    public ModificationResult<O> removeByPrimaryKey(Object object, QueryOptions queryOptions) {
        final O existingObject = getObjectByPrimaryKey(object, queryOptions);
        if (existingObject == null) {
            return ModificationResult.notFound();
        }
        backingIndex.removeAll(ObjectSet.fromCollection(Collections.singleton(existingObject)), queryOptions);
        return ModificationResult.removed(existingObject);
    }

    @Override
    public boolean containsAll(Collection<?> c, QueryOptions queryOptions) {
        for (Object o : c) {
            if (!contains(o, queryOptions)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends O> c, QueryOptions queryOptions) {
        boolean modified = false;
        for (O object : normalizeObjectsByPrimaryKeyLastWins(c, queryOptions).values()) {
            modified = addOrReplace(object, queryOptions).isModified() || modified;
        }
        return modified;
    }

    @Override
    public boolean retainAll(Collection<?> c, QueryOptions queryOptions) {
        final Map<A, O> objectsToRetain = normalizeObjectsByPrimaryKeyFirstWins(c, queryOptions);
        final Collection<O> objectsToRemove = new ArrayList<O>();
        ResultSet<O> allObjects = backingIndex.retrieve(has(primaryKeyAttribute), queryOptions);
        try {
            for (O object : allObjects) {
                final A objectId = primaryKeyAttribute.getValue(object, queryOptions);
                if (!objectsToRetain.containsKey(objectId)) {
                    objectsToRemove.add(object);
                }
            }
        }
        finally {
            allObjects.close();
        }
        boolean modified = false;
        for (O objectToRemove : objectsToRemove) {
            modified = removeByPrimaryKey(objectToRemove, queryOptions).isModified() || modified;
        }
        return modified;
    }

    @Override
    public boolean removeAll(Collection<?> c, QueryOptions queryOptions) {
        boolean modified = false;
        for (Object object : normalizeObjectsByPrimaryKeyFirstWins(c, queryOptions).values()) {
            modified = removeByPrimaryKey(object, queryOptions).isModified() || modified;
        }
        return modified;
    }

    @Override
    public void clear(QueryOptions queryOptions) {
        backingIndex.clear(queryOptions);
    }

    A extractPrimaryKey(O object, QueryOptions queryOptions) {
        if (object == null) {
            throw new NullPointerException("Object was null");
        }
        final A primaryKey = primaryKeyAttribute.getValue(object, queryOptions);
        if (primaryKey == null) {
            throw new IllegalStateException("Primary key attribute returned null for object: " + object);
        }
        return primaryKey;
    }

    A extractPrimaryKeyOrNull(Object object, QueryOptions queryOptions) {
        if (object == null) {
            return null;
        }
        try {
            @SuppressWarnings("unchecked")
            final O typedObject = (O) object;
            return extractPrimaryKey(typedObject, queryOptions);
        }
        catch (ClassCastException e) {
            return null;
        }
    }

    Map<A, O> normalizeObjectsByPrimaryKeyFirstWins(Collection<?> objects, QueryOptions queryOptions) {
        final Map<A, O> normalizedObjects = new LinkedHashMap<A, O>();
        for (Object object : objects) {
            final A primaryKey = extractPrimaryKeyOrNull(object, queryOptions);
            if (primaryKey == null || normalizedObjects.containsKey(primaryKey)) {
                continue;
            }
            @SuppressWarnings("unchecked")
            final O typedObject = (O) object;
            normalizedObjects.put(primaryKey, typedObject);
        }
        return normalizedObjects;
    }

    Map<A, O> normalizeObjectsByPrimaryKeyLastWins(Collection<? extends O> objects, QueryOptions queryOptions) {
        final Map<A, O> normalizedObjects = new LinkedHashMap<A, O>();
        for (O object : objects) {
            normalizedObjects.put(extractPrimaryKey(object, queryOptions), object);
        }
        return normalizedObjects;
    }
}
