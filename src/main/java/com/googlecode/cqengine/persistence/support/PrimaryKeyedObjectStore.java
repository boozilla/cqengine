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
package com.googlecode.cqengine.persistence.support;

import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;

/**
 * An {@link ObjectStore} whose identity is defined by a primary key rather than by object equality.
 *
 * @author niall.gallagher
 */
public interface PrimaryKeyedObjectStore<O> extends ObjectStore<O> {

    SimpleAttribute<O, ? extends Comparable> getPrimaryKeyAttribute();

    Object getPrimaryKeyForObject(Object object, QueryOptions queryOptions);

    O getObjectByPrimaryKey(Object object, QueryOptions queryOptions);

    ModificationResult<O> addOrReplace(O object, QueryOptions queryOptions);

    ModificationResult<O> removeByPrimaryKey(Object object, QueryOptions queryOptions);

    final class ModificationResult<O> {
        final O previousObject;
        final O currentObject;
        final boolean modified;

        ModificationResult(O previousObject, O currentObject, boolean modified) {
            this.previousObject = previousObject;
            this.currentObject = currentObject;
            this.modified = modified;
        }

        public O getPreviousObject() {
            return previousObject;
        }

        public O getCurrentObject() {
            return currentObject;
        }

        public boolean isModified() {
            return modified;
        }

        public static <O> ModificationResult<O> inserted(O currentObject) {
            return new ModificationResult<O>(null, currentObject, true);
        }

        public static <O> ModificationResult<O> replaced(O previousObject, O currentObject) {
            return new ModificationResult<O>(previousObject, currentObject, true);
        }

        public static <O> ModificationResult<O> unchanged(O currentObject) {
            return new ModificationResult<O>(currentObject, currentObject, false);
        }

        public static <O> ModificationResult<O> removed(O previousObject) {
            return new ModificationResult<O>(previousObject, null, true);
        }

        public static <O> ModificationResult<O> notFound() {
            return new ModificationResult<O>(null, null, false);
        }
    }
}
