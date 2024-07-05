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
package io.trino.spi.block;

import java.util.Arrays;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public enum BlockEncodingId
{
    ARRAY((short) 0),
    BYTE((short) 1),
    DICTIONARY((short) 2),
    FIXED_12((short) 3),
    INT128((short) 4),
    INT_ARRAY((short) 5),
    LAZY((short) 6),
    LONG_ARRAY((short) 7),
    MAP((short) 8),
    ROW((short) 9),
    RLE((short) 10),
    SHORT_ARRAY((short) 11),
    VARIABLE_WIDTH((short) 12),
    /**/;

    private static Map<Short, BlockEncodingId> reverseMapping = Arrays.stream(values())
            .map(value -> Map.entry(value.id(), value))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

    private final short id;

    BlockEncodingId(short id)
    {
        this.id = id;
    }

    public short id()
    {
        return id;
    }

    public static BlockEncodingId forId(short id)
    {
        BlockEncodingId mapping = reverseMapping.get(id);
        if (mapping == null) {
            throw new IllegalArgumentException("Unknown block encoding id: " + id);
        }
        return mapping;
    }
}
