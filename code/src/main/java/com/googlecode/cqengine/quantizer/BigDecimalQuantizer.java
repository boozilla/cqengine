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

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * A static factory for creating {@link Quantizer}s for {@link java.math.BigDecimal} attributes.
 * <p/>
 * See {@link #withCompressionFactor(int)} for details.
 *
 * @author Niall Gallagher
 */
public class BigDecimalQuantizer {

    /**
     * Private constructor, not used.
     */
    BigDecimalQuantizer() {
    }

    static class TruncatingAndCompressingQuantizer implements Quantizer<BigDecimal> {
        private final BigDecimal compressionFactor;

        public TruncatingAndCompressingQuantizer(int compressionFactor) {
            if (compressionFactor < 2) {
                throw new IllegalArgumentException("Invalid compression factor, must be >= 2: " + compressionFactor);
            }
            this.compressionFactor = BigDecimal.valueOf(compressionFactor);
        }

        @Override
        public BigDecimal getQuantizedValue(BigDecimal attributeValue) {
            return attributeValue
                    .divideToIntegralValue(compressionFactor)
                    .multiply(compressionFactor)
                    .setScale(0, RoundingMode.DOWN);
        }
    }

    static class TruncatingQuantizer implements Quantizer<BigDecimal> {

        @Override
        public BigDecimal getQuantizedValue(BigDecimal attributeValue) {
            return attributeValue.setScale(0, RoundingMode.DOWN);
        }
    }


    /**
     * Returns a {@link Quantizer} which converts the input value to the nearest multiple of the compression
     * factor, in the direction towards zero.
     * <p/>
     * &lt;b&gt;Examples (compression factor 5):&lt;/b&gt;&lt;br/&gt;
     * &lt;ul&gt;
     *     &lt;li&gt;Input value 0.0 -&gt; 0&lt;/li&gt;
     *     &lt;li&gt;Input value 4.2 -&gt; 0&lt;/li&gt;
     *     &lt;li&gt;Input value 5.0 -&gt; 5&lt;/li&gt;
     *     &lt;li&gt;Input value 9.9 -&gt; 5&lt;/li&gt;
     *     &lt;li&gt;Input value -0.0 -&gt; 0&lt;/li&gt;
     *     &lt;li&gt;Input value -4.2 -&gt; 0&lt;/li&gt;
     *     &lt;li&gt;Input value -5.0 -&gt; -5&lt;/li&gt;
     *     &lt;li&gt;Input value -9.9 -&gt; -5&lt;/li&gt;
     * &lt;/ul&gt;
     *
     * @param compressionFactor The number of adjacent mathematical integers to coalesce to a single key. &lt;b&gt;Supply a
     * factor &lt; 2 to disable compression&lt;/b&gt; and simply truncate everything after the decimal point
     * @return A {@link Quantizer} which converts the input value to the closest multiple of the compression
     * factor, in the direction towards zero
     */
    public static Quantizer<BigDecimal> withCompressionFactor(int compressionFactor) {
        return compressionFactor < 2 ? new TruncatingQuantizer() : new TruncatingAndCompressingQuantizer(compressionFactor);
    }
}
