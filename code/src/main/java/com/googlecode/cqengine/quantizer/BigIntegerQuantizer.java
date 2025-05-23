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
package com.googlecode.cqengine.quantizer;

import java.math.BigInteger;

/**
 * A static factory for creating {@link Quantizer}s for {@link java.math.BigInteger} attributes.
 * <p/>
 * See {@link #withCompressionFactor(int)} for details.
 *
 * @author Niall Gallagher
 */
public class BigIntegerQuantizer {

    /**
     * Private constructor, not used.
     */
    BigIntegerQuantizer() {
    }

    static class CompressingQuantizer implements Quantizer<BigInteger> {
        private final BigInteger compressionFactor;

        public CompressingQuantizer(int compressionFactor) {
            if (compressionFactor < 2) {
                throw new IllegalArgumentException("Invalid compression factor, must be >= 2: " + compressionFactor);
            }
            this.compressionFactor = BigInteger.valueOf(compressionFactor);
        }

        @Override
        public BigInteger getQuantizedValue(BigInteger attributeValue) {
            return attributeValue
                    .divide(compressionFactor)
                    .multiply(compressionFactor);
        }
    }

    /**
     * Returns a {@link Quantizer} which converts the input value to the nearest multiple of the compression
     * factor, in the direction towards zero.
     * <p/>
     * &lt;b&gt;Examples (compression factor 5):&lt;/b&gt;&lt;br/&gt;
     * &lt;ul&gt;
     *     &lt;li&gt;Input value 0 -&gt; 0&lt;/li&gt;
     *     &lt;li&gt;Input value 4 -&gt; 0&lt;/li&gt;
     *     &lt;li&gt;Input value 5 -&gt; 5&lt;/li&gt;
     *     &lt;li&gt;Input value 9 -&gt; 5&lt;/li&gt;
     *     &lt;li&gt;Input value -4 -&gt; 0&lt;/li&gt;
     *     &lt;li&gt;Input value -5 -&gt; -5&lt;/li&gt;
     *     &lt;li&gt;Input value -9 -&gt; -5&lt;/li&gt;
     * &lt;/ul&gt;
     *
     * @param compressionFactor The number of adjacent mathematical integers (&gt;= 2) to coalesce to a single key
     * @return A {@link Quantizer} which converts the input value to the nearest multiple of the compression
     * factor, in the direction towards zero
     */
    public static Quantizer<BigInteger> withCompressionFactor(int compressionFactor) {
        return new CompressingQuantizer(compressionFactor);
    }
}
