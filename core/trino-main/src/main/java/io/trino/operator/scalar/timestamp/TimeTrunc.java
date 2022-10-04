/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.scalar.timestamp;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.StandardTypes;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import static io.trino.operator.scalar.DateTimeFunctions.getTimestampField;
import static io.trino.type.DateTimes.scaleEpochMicrosToMillis;
import static io.trino.type.DateTimes.scaleEpochMillisToMicros;
import static java.lang.Math.toIntExact;

@Description("Truncate to the specified precision in the session timezone")
@ScalarFunction("time_trunc")
public final class TimeTrunc
{
    private TimeTrunc() {}

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static long truncate(
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType("timestamp(p)") long timestamp)
    {
        long epochMillis = scaleEpochMicrosToMillis(timestamp);
        DateTimeField timeField = getTimestampField(ISOChronology.getInstanceUTC(), unit);

        int truncatedValue = toIntExact(timeField.get(epochMillis) / value * value);
        long result = timeField.roundFloor(timeField.set(epochMillis, truncatedValue));

        return scaleEpochMillisToMicros(result);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static LongTimestamp truncate(
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        long epochMillis = scaleEpochMicrosToMillis(timestamp.getEpochMicros());
        DateTimeField timeField = getTimestampField(ISOChronology.getInstanceUTC(), unit);

        int truncatedValue = toIntExact(timeField.get(epochMillis) / value * value);
        long result = timeField.roundFloor(timeField.set(epochMillis, truncatedValue));

        // smallest unit of truncation is "millisecond", so the fraction is always 0
        return new LongTimestamp(scaleEpochMillisToMicros(result), 0);
    }
}
