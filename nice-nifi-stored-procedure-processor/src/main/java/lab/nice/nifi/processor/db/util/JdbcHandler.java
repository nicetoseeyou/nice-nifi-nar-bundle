package lab.nice.nifi.processor.db.util;

import lab.nice.nifi.processor.db.common.Parameter;
import lab.nice.nifi.processor.db.common.ParameterType;
import org.apache.commons.lang3.StringUtils;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.ZoneId;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Handler to apply parameters and register parameters to JDBC PreparedStatement and CallableStatement
 * based on built-in parameters.
 */
public final class JdbcHandler {
    private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    private static final String DEFAULT_TIME_FORMAT = "HH:mm:ss.SSS";
    private static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    private static final Pattern LONG_PATTERN = Pattern.compile("^-?\\d{1,19}$");

    private JdbcHandler() {

    }

    /**
     * Set stored procedure IN/OUT/INOUT parameters to CallableStatement based on the built in parameter map.
     *
     * @param statement    the CallableStatement to set
     * @param lobHandler   the handler for CLOB/NCLOB value
     * @param parameterMap the built in parameter map
     * @throws ParseException parsing unsupported binary data (except ascii, hex, base64)
     * @throws SQLException   if failed to apply parameter or register output parameter to CallableStatement
     * @throws IOException    if failed to read character stream or binary stream
     */
    public static void setParameters(final CallableStatement statement, final LobHandler lobHandler,
                                     final Map<Integer, Parameter> parameterMap)
            throws ParseException, SQLException, IOException {
        for (Map.Entry<Integer, Parameter> entry : parameterMap.entrySet()) {
            applyParameter(statement, lobHandler, entry.getValue());
            registerOutput(statement, entry.getValue());
        }
    }

    /**
     * Apply statement parameters to PreparedStatement based on built in parameter.
     *
     * @param statement  the PreparedStatement to apply
     * @param lobHandler the handler for CLOB/NCLOB value
     * @param parameter  the built in parameter
     * @throws SQLException   if failed to apply statement parameter to PreparedStatement
     * @throws ParseException parsing unsupported binary data (except ascii, hex, base64)
     * @throws IOException    if failed to read character stream or binary stream
     */
    public static void applyParameter(final PreparedStatement statement, final LobHandler lobHandler, final Parameter parameter)
            throws SQLException, ParseException, IOException {
        if (ParameterType.IN.equals(parameter.getType()) || ParameterType.INOUT.equals(parameter.getType())) {
            if (null == parameter.getValue()) {
                statement.setNull(parameter.getIndex(), parameter.getJdbcType().getVendorTypeNumber());
            } else {
                switch (parameter.getJdbcType()) {
                    case BIT:
                        statement.setBoolean(parameter.getIndex(),
                                "1".equals(parameter.getValue())
                                        || "t".equalsIgnoreCase(parameter.getValue())
                                        || Boolean.parseBoolean(parameter.getValue()));
                        break;
                    case BOOLEAN:
                        statement.setBoolean(parameter.getIndex(), Boolean.parseBoolean(parameter.getValue()));
                        break;
                    case TINYINT:
                        statement.setByte(parameter.getIndex(), Byte.parseByte(parameter.getValue()));
                        break;
                    case SMALLINT:
                        statement.setShort(parameter.getIndex(), Short.parseShort(parameter.getValue()));
                        break;
                    case INTEGER:
                        statement.setInt(parameter.getIndex(), Integer.parseInt(parameter.getValue()));
                        break;
                    case BIGINT:
                        statement.setLong(parameter.getIndex(), Long.parseLong(parameter.getValue()));
                        break;
                    case REAL:
                        statement.setFloat(parameter.getIndex(), Float.parseFloat(parameter.getValue()));
                        break;
                    case FLOAT:
                    case DOUBLE:
                        statement.setDouble(parameter.getIndex(), Double.parseDouble(parameter.getValue()));
                        break;
                    case DECIMAL:
                    case NUMERIC:
                        statement.setBigDecimal(parameter.getIndex(), new BigDecimal(parameter.getValue()));
                        break;
                    case DATE:
                        final long epochOfDate;
                        if (StringUtils.isBlank(parameter.getFormat())) {
                            if (LONG_PATTERN.matcher(parameter.getValue()).matches()) {
                                epochOfDate = Long.parseLong(parameter.getValue());
                            } else {
                                epochOfDate = TimeUtility.dateToEpochMilli(parameter.getValue(), DEFAULT_DATE_FORMAT,
                                        ZoneId.systemDefault().getId());

                            }
                        } else {
                            epochOfDate = TimeUtility.dateToEpochMilli(parameter.getValue(), parameter.getFormat(),
                                    ZoneId.systemDefault().getId());
                        }
                        statement.setDate(parameter.getIndex(), new Date(epochOfDate));
                        break;
                    case TIME:
                        final long epochOfTime;
                        if (StringUtils.isBlank(parameter.getFormat())) {
                            if (LONG_PATTERN.matcher(parameter.getValue()).matches()) {
                                epochOfTime = Long.parseLong(parameter.getValue());
                            } else {
                                epochOfTime = TimeUtility.timeToEpochMilli(parameter.getValue(), DEFAULT_TIME_FORMAT,
                                        ZoneId.systemDefault().getId());
                            }
                        } else {
                            epochOfTime = TimeUtility.timeToEpochMilli(parameter.getValue(), parameter.getFormat(),
                                    ZoneId.systemDefault().getId());
                        }
                        statement.setTime(parameter.getIndex(), new Time(epochOfTime));
                        break;
                    case TIMESTAMP:
                        final long epochOfTs;
                        if (StringUtils.isBlank(parameter.getFormat())) {
                            if (LONG_PATTERN.matcher(parameter.getValue()).matches()) {
                                epochOfTs = Long.parseLong(parameter.getValue());
                            } else {
                                epochOfTs = TimeUtility.timestampToEpochMilli(parameter.getValue(), DEFAULT_TIMESTAMP_FORMAT,
                                        ZoneId.systemDefault().getId());
                            }
                        } else {
                            epochOfTs = TimeUtility.timestampToEpochMilli(parameter.getValue(), parameter.getFormat(),
                                    ZoneId.systemDefault().getId());
                        }
                        statement.setTimestamp(parameter.getIndex(), new Timestamp(epochOfTs));
                        break;
                    case BINARY:
                    case VARBINARY:
                    case LONGVARBINARY:
                        final byte[] bytes;
                        if (StringUtils.isBlank(parameter.getFormat())) {
                            bytes = parameter.getValue().getBytes(StandardCharsets.US_ASCII);
                        } else {
                            switch (parameter.getFormat()) {
                                case "ascii":
                                    bytes = parameter.getValue().getBytes(StandardCharsets.US_ASCII);
                                    break;
                                case "hex":
                                    bytes = DatatypeConverter.parseHexBinary(parameter.getValue());
                                    break;
                                case "base64":
                                    bytes = DatatypeConverter.parseBase64Binary(parameter.getValue());
                                    break;
                                default:
                                    throw new ParseException("Unable to parse binary data using the formatter `" + parameter.getFormat() + "`.", 0);
                            }
                        }
                        statement.setBinaryStream(parameter.getIndex(), new ByteArrayInputStream(bytes), bytes.length);
                        break;
                    case CHAR:
                    case VARCHAR:
                    case LONGVARCHAR:
                        statement.setString(parameter.getIndex(), parameter.getValue());
                        break;
                    case NCHAR:
                    case NVARCHAR:
                    case LONGNVARCHAR:
                        statement.setNString(parameter.getIndex(), parameter.getValue());
                        break;
                    case CLOB:
                        try (final Reader reader = new StringReader(parameter.getValue())) {
                            final Clob clob = lobHandler.clob(reader);
                            statement.setClob(parameter.getIndex(), clob);
                        }
                        break;
                    case NCLOB:
                        try (final Reader reader = new StringReader(parameter.getValue())) {
                            final NClob nClob = lobHandler.nClob(reader);
                            statement.setNClob(parameter.getIndex(), nClob);
                        }
                        break;
                    default:
                        statement.setObject(parameter.getIndex(), parameter.getValue(), parameter.getJdbcType().getVendorTypeNumber());
                }
            }
        } else {
            return;
        }
    }

    /**
     * Register stored procedure output parameter in CallableStatement based on built-in parameter
     *
     * @param statement the CallableStatement to register
     * @param parameter the built in parameter
     * @throws SQLException if failed to register output parameter
     */
    public static void registerOutput(final CallableStatement statement, final Parameter parameter) throws SQLException {
        if (ParameterType.OUT.equals(parameter.getType()) || ParameterType.INOUT.equals(parameter.getType())) {
            statement.registerOutParameter(parameter.getIndex(), parameter.getJdbcType().getVendorTypeNumber());
        } else {
            return;
        }
    }
}
