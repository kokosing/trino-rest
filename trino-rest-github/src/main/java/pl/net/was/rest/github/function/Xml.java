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

package pl.net.was.rest.github.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;

import java.io.IOException;

import static io.trino.spi.type.StandardTypes.VARCHAR;

@ScalarFunction("xml2json")
@Description("Convert XML to JSON")
public class Xml
{
    @SqlType(VARCHAR)
    public Slice xml2json(@SqlType(VARCHAR) Slice xml)
            throws IOException
    {
        XmlMapper xmlMapper = new XmlMapper();
        JsonNode node = xmlMapper.readTree(xml.getBytes());

        ObjectMapper jsonMapper = new ObjectMapper();
        String json = jsonMapper.writeValueAsString(node);
        return Slices.utf8Slice(json);
    }
}
