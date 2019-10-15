package lab.nice.nifi.processor.db.util;

import com.fasterxml.jackson.core.JsonGenerator;
import lab.nice.nifi.processor.db.common.Parameter;
import lab.nice.nifi.processor.db.common.ParameterType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Handler to retrieve CallableStatement/ResultSet to write to JSON.
 */
public final class JsonHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonHandler.class);

    private static final String RESULT_SET_HEADER = "RESULTS";
    private static final String OUTPUT_HEADER = "OUTPUTS";
    private static final String OUTPUT_PREFIX = "output_";
    private static final int BUFFER_SIZE = 256;

    private JsonHandler() {
    }

    /**
     * Streaming retrieve ResultSet(s) and output(s) inside a CallableStatement and write into JSON.
     * All ResultSet(s) will be written into JSON wrapping array with field name {@link JsonHandler#RESULT_SET_HEADER} and
     * each ResultSet will be written into JSON array inside this wrapping array. Rows inside ResultSet will be written as
     * JSON object with all its select columns. And update count will be written as simple number in the top of the ResultSet
     * wrapping array. Output(s) of the CallableStatement will be written in a wrapping JSON object with field name
     * {@link JsonHandler#OUTPUT_HEADER} and each output will be written as JSON with field name (if not specified, a default name
     * with prefix {@link JsonHandler#OUTPUT_PREFIX} with its index will be assigned).
     *
     * <pre>
     * {
     * 	"Results": [
     * 		[{"ID": 1, "NAME": "Tom", "AGE": 21}],
     * 		[{"ID": 2, "CITY": "Guangzhou"}]
     * 	],
     * 	"Outputs": {
     * 		"ID": 2,
     * 		"output_3": "output_3"
     *        }
     * }
     * </pre>
     *
     * @param statement     the CallableStatement to retrieve
     * @param jsonGenerator the JsonGenerator to write JSON
     * @param parameters    the parameter list
     * @throws IOException  if failed to retrieve CLOB/NCLOB output if any or failed to write JSON
     * @throws SQLException if failed to retrieve outputs
     */
    public static void retrieveCallableStatement(final CallableStatement statement, final JsonGenerator jsonGenerator,
                                                 final List<Parameter> parameters) throws IOException, SQLException {
        retrieveResults(statement, jsonGenerator);
        retrieveOutputs(statement, jsonGenerator, parameters);
    }

    /**
     * Streaming retrieve ResultSet(s) and output(s) inside a CallableStatement and write into JSON.
     * All ResultSet(s) will be written into JSON wrapping array with field name {@link JsonHandler#RESULT_SET_HEADER} and
     * each ResultSet will be written into JSON array inside this wrapping array. Rows inside ResultSet will be written as
     * JSON object with all its select columns. And update count will be written as simple number in the top of the ResultSet
     * wrapping array. Output(s) of the CallableStatement will be written in a wrapping JSON object with field name
     * {@link JsonHandler#OUTPUT_HEADER} and each output will be written as JSON with field name (if not specified, a default name
     * with prefix {@link JsonHandler#OUTPUT_PREFIX} with its index will be assigned).
     *
     * <pre>
     * {
     * 	"Results": [
     * 		[{"ID": 1, "NAME": "Tom", "AGE": 21}],
     * 		[{"ID": 2, "CITY": "Guangzhou"}]
     * 	],
     * 	"Outputs": {
     * 		"ID": 2,
     * 		"output_3": "output_3"
     *        }
     * }
     * </pre>
     *
     * @param statement     the CallableStatement to retrieve
     * @param jsonGenerator the JsonGenerator to write JSON
     * @param parameterMap  the parameters map
     * @throws IOException  if failed to retrieve CLOB/NCLOB output if any or failed to write JSON
     * @throws SQLException if failed to retrieve outputs
     */
    public static void retrieveCallableStatement(final CallableStatement statement, final JsonGenerator jsonGenerator,
                                                 final Map<Integer, Parameter> parameterMap) throws IOException, SQLException {
        final List<Parameter> parameters = new ArrayList<>(parameterMap.values());
        /* start of root */
        jsonGenerator.writeStartObject();
        retrieveResults(statement, jsonGenerator);
        retrieveOutputs(statement, jsonGenerator, parameters);
        jsonGenerator.writeEndObject();
        //end of root
    }

    /**
     * Streaming retrieve CallableStatement outputs based on parameters. If any OUT/INOUT parameter given,
     * will retrieve this OUT/INOUT parameter and write to JSON. If no name specified for the parameter,
     * a default name with prefix {@link JsonHandler#OUTPUT_PREFIX} and its index will be assigned.
     *
     * @param statement     the CallableStatement to retrieve
     * @param jsonGenerator the JsonGenerator to write JSON
     * @param parameters    the parameter list
     * @throws IOException  if failed to retrieve CLOB/NCLOB output if any or failed to write JSON
     * @throws SQLException if failed to retrieve outputs
     */
    public static void retrieveOutputs(final CallableStatement statement, final JsonGenerator jsonGenerator,
                                       final List<Parameter> parameters) throws IOException, SQLException {
        if (null != parameters && !parameters.isEmpty()) {
            boolean isEmpty = true;
            for (Parameter parameter : parameters) {
                if (ParameterType.OUT.equals(parameter.getType())
                        || ParameterType.INOUT.equals(parameter.getType())) {
                    if (isEmpty) {
                        jsonGenerator.writeObjectFieldStart(OUTPUT_HEADER);
                        isEmpty = false;
                    }
                    String fieldName = parameter.getName();
                    if (StringUtils.isBlank(fieldName)) {
                        fieldName = OUTPUT_PREFIX + parameter.getIndex();
                    }
                    if (Types.CLOB == parameter.getJdbcType().getVendorTypeNumber()) {
                        writeJson(jsonGenerator, fieldName, statement.getCharacterStream(parameter.getIndex()));
                    } else if (Types.NCLOB == parameter.getJdbcType().getVendorTypeNumber()) {
                        writeJson(jsonGenerator, fieldName, statement.getNCharacterStream(parameter.getIndex()));
                    } else {
                        writeJson(jsonGenerator, fieldName, statement.getObject(parameter.getIndex()));
                    }
                }
            }
            if (!isEmpty) {
                jsonGenerator.writeEndObject();
            }
        }
    }

    /**
     * Streaming retrieve ResultSet and update count of Statement into JSON.
     *
     * @param statement     the statement to retrieve
     * @param jsonGenerator the JSON writer
     * @throws SQLException if failed to retrieve statement
     * @throws IOException  if failed to read CLOB/NCLOB value in ResultSet or failed to write JSON content
     */
    public static void retrieveResults(final Statement statement, final JsonGenerator jsonGenerator)
            throws SQLException, IOException {
        boolean hasResults = hasMoreResults(statement);
        if (hasResults) {
            //start of results
            jsonGenerator.writeArrayFieldStart(RESULT_SET_HEADER);
            while (hasResults) {
                if (statement.getUpdateCount() != -1) {
                    jsonGenerator.writeObject(statement.getUpdateCount());
                } else {
                    retrieveResultSet(statement.getResultSet(), jsonGenerator);
                }
                hasResults = hasMoreResults(statement);
            }
            jsonGenerator.writeEndArray();
            //end of results
        }
    }

    /**
     * Streaming retrieve ResultSet and write into JSON, empty ResultSet will be skipped.
     *
     * @param resultSet     the ResultSet to retrieve
     * @param jsonGenerator the JSON writer
     * @throws SQLException if failed to retrieve ResultSet
     * @throws IOException  if failed to read CLOB/NCLOB value in ResultSet or failed to write JSON content
     */
    public static void retrieveResultSet(final ResultSet resultSet, final JsonGenerator jsonGenerator)
            throws SQLException, IOException {
        if (resultSet.isBeforeFirst()) {
            final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            final int columnCount = resultSetMetaData.getColumnCount();
            //start of ResultSet
            jsonGenerator.writeStartArray();
            while (resultSet.next()) {
                //start of row
                jsonGenerator.writeStartObject();
                for (int i = 1; i <= columnCount; i++) {
                    if (resultSetMetaData.getColumnType(i) == Types.CLOB) {
                        writeJson(jsonGenerator, resultSetMetaData.getColumnName(i), resultSet.getCharacterStream(i));
                    } else if (resultSetMetaData.getColumnType(i) == Types.NCLOB) {
                        writeJson(jsonGenerator, resultSetMetaData.getColumnName(i), resultSet.getNCharacterStream(i));
                    } else {
                        writeJson(jsonGenerator, resultSetMetaData.getColumnName(i), resultSet.getObject(i));
                    }
                }
                jsonGenerator.writeEndObject();
                //end of Row
            }
            jsonGenerator.writeEndArray();
            //end of ResultSet
        } else {
            //empty ResultSet
            LOGGER.warn("Empty ResultSet, will skip it");
        }
    }

    /**
     * Write JSON from character stream. JSON NULL will be written for NULL stream.
     *
     * @param jsonGenerator the JSON writer
     * @param fieldName     the field name of the JSON value
     * @param reader        the character stream
     * @throws IOException if failed to read character stream or failed to write JSON content
     */
    public static void writeJson(final JsonGenerator jsonGenerator, final String fieldName, final Reader reader)
            throws IOException {
        if (null == reader) {
            jsonGenerator.writeNullField(fieldName);
        } else {
            final StringBuffer stringBuffer = new StringBuffer();
            final CharBuffer charBuffer = CharBuffer.allocate(BUFFER_SIZE);
            while (reader.read(charBuffer) != -1) {
                charBuffer.flip();
                stringBuffer.append(charBuffer.toString());
                charBuffer.clear();
            }
            jsonGenerator.writeStringField(fieldName, stringBuffer.toString());
        }
    }

    /**
     * Write Java Object to JSON, JSON NULL will be written for NULL Java object.
     *
     * @param jsonGenerator the JSON writer
     * @param fieldName     the field name of the JSON value
     * @param value         the Json value
     * @throws IOException if failed to write JSON content or unsupported Java type
     */
    public static void writeJson(final JsonGenerator jsonGenerator, final String fieldName, final Object value)
            throws IOException {
        if (null == value) {
            jsonGenerator.writeNullField(fieldName);
        } else {
            jsonGenerator.writeObjectField(fieldName, value);
        }
    }

    private static boolean hasMoreResults(final Statement statement) {
        try {
            return statement.getMoreResults() || statement.getUpdateCount() != -1;
        } catch (SQLException e) {
            LOGGER.warn("No more ResultSet, will return false.", e);
            return false;
        }
    }
}
