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
package io.trino.operator.scalar.timestamptz;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import static io.trino.operator.scalar.DateTimeFunctions.getTimestampField;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.updateMillisUtc;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.util.DateTimeZoneIndex.getChronology;
import static io.trino.util.DateTimeZoneIndex.unpackChronology;
import static java.lang.Math.toIntExact;

@Description("Truncate to the specified precision")
@ScalarFunction("time_trunc")
public final class TimeTrunc
{
    private TimeTrunc() {}

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static long truncate(
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType("timestamp(p) with time zone") long packedEpochMillis)
    {
        ISOChronology chronology = unpackChronology(packedEpochMillis);
        long epochMillis = unpackMillisUtc(packedEpochMillis);
        DateTimeField timeField = getTimestampField(chronology, unit);

        int truncatedValue = toIntExact(timeField.get(epochMillis) / value * value);
        long result = timeField.roundFloor(timeField.set(epochMillis, truncatedValue));

        return updateMillisUtc(result, packedEpochMillis);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static LongTimestampWithTimeZone truncate(
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
    {
        ISOChronology chronology = getChronology(getTimeZoneKey(timestamp.getTimeZoneKey()));
        long epochMillis = timestamp.getEpochMillis();
        DateTimeField timeField = getTimestampField(chronology, unit);

        int truncatedValue = toIntExact(timeField.get(epochMillis) / value * value);
        long result = timeField.roundFloor(timeField.set(epochMillis, truncatedValue));

        // smallest unit of truncation is "millisecond", so the fraction is always 0
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(result, 0, timestamp.getTimeZoneKey());
    }
}
