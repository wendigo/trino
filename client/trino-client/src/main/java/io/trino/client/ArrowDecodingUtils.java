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
package io.trino.client;

import org.apache.arrow.vector.util.Text;

import java.time.LocalDate;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.ClientStandardTypes.BOOLEAN;
import static io.trino.client.ClientStandardTypes.DATE;
import static io.trino.client.ClientStandardTypes.DOUBLE;
import static io.trino.client.ClientStandardTypes.INTEGER;
import static io.trino.client.ClientStandardTypes.REAL;
import static io.trino.client.ClientStandardTypes.SMALLINT;
import static io.trino.client.ClientStandardTypes.TINYINT;
import static io.trino.client.ClientStandardTypes.VARCHAR;

public class ArrowDecodingUtils
{
    private static final TypeDecoder PASS_THROUGH_DECODER = new PassThroughDecoder();
    private static final TypeDecoder VARCHAR_DECODER = new VarcharDecoder();

    private ArrowDecodingUtils()
    {
    }

    public static TypeDecoder[] createTypeDecoders(List<Column> columns)
    {
        verify(!columns.isEmpty(), "Columns must not be empty");
        TypeDecoder[] decoders = new TypeDecoder[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            decoders[i] = createTypeDecoder(columns.get(i).getTypeSignature());
        }
        return decoders;
    }

    private static TypeDecoder createTypeDecoder(ClientTypeSignature signature)
    {
        switch (signature.getRawType()) {
            case BIGINT:
                return PASS_THROUGH_DECODER;
            case INTEGER:
                return PASS_THROUGH_DECODER;
            case SMALLINT:
                return PASS_THROUGH_DECODER;
            case TINYINT:
                return PASS_THROUGH_DECODER;
            case DOUBLE:
                return PASS_THROUGH_DECODER;
            case REAL:
                return PASS_THROUGH_DECODER;
            case BOOLEAN:
                return PASS_THROUGH_DECODER;
            case VARCHAR:
                return VARCHAR_DECODER;
//            case ARRAY:
//                return new JsonDecodingUtils.ArrayDecoder(signature);
//            case MAP:
//                return new JsonDecodingUtils.MapDecoder(signature);
//            case ROW:
//                return new JsonDecodingUtils.RowDecoder(signature);
//            case VARCHAR:
//            case JSON:
//            case TIME:
//            case TIME_WITH_TIME_ZONE:
//            case TIMESTAMP:
//            case TIMESTAMP_WITH_TIME_ZONE:
            case DATE:
                return arrowValue -> LocalDate.ofEpochDay((int) arrowValue).toString();
//            case INTERVAL_YEAR_TO_MONTH:
//            case INTERVAL_DAY_TO_SECOND:
//            case IPADDRESS:
//            case UUID:
//            case DECIMAL:
//            case CHAR:
//            case GEOMETRY:
//            case SPHERICAL_GEOGRAPHY:
//            case COLOR:
//                return STRING_DECODER;
//            case KDB_TREE:
//            case BING_TILE:
//                return OBJECT_DECODER;
//            case QDIGEST:
//            case P4_HYPER_LOG_LOG:
//            case HYPER_LOG_LOG:
//            case SET_DIGEST:
//            case VARBINARY:
            default:
                return PASS_THROUGH_DECODER;
        }
    }

    private static class PassThroughDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(Object arrowValue)
        {
            return arrowValue;
        }
    }

    private static class VarcharDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(Object arrowValue)
        {
            verify(arrowValue instanceof Text, "Expected value to be Arrow text");
            return arrowValue.toString();
        }
    }

    public interface TypeDecoder
    {
        Object decode(Object arrowValue);
    }
}
