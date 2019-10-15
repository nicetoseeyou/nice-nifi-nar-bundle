package lab.nice.nifi.processor.db.util;



import lab.nice.nifi.processor.db.common.AttributeConstant;
import lab.nice.nifi.processor.db.common.Parameter;
import lab.nice.nifi.processor.db.common.ParameterType;
import lab.nice.nifi.processor.db.common.ParameterTypes;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.regex.Matcher;

/**
 * Handler to retrieve stored procedure parameters from NiFi processor properties or FlowFile attributes.
 * All stored procedure parameters will be wrapped into built-in parameters.
 */
public final class AttributeHandler {
    private AttributeHandler() {
    }

    /**
     * Retrieve stored procedure parameter from NiFi processor properties or NiFi FlowFile attributes.
     *
     * @param attributes NiFi processor properties or NiFi FlowFile attributes
     * @param parameters the target stored procedure parameter map
     */
    public static void retrieveProcedureParameter(final Map<String, String> attributes,
                                                  final Map<Integer, Parameter> parameters) {
        if (null != attributes && !attributes.isEmpty()) {
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                final String attributeName = entry.getKey();
                final Matcher matcher = AttributeConstant.PROCEDURE_TYPE_ATTRIBUTE.matcher(attributeName);
                if (matcher.matches()) {
                    final String parameterType = matcher.group(1);
                    final Integer parameterIndex = Integer.parseInt(matcher.group(2));
                    final Integer jdbcType = Integer.parseInt(entry.getValue());

                    Parameter parameter = parameters.get(parameterIndex);

                    if (parameter == null) {
                        parameter = new Parameter(parameterType, parameterIndex, jdbcType);
                        parameters.put(parameterIndex, parameter);
                    } else {
                        parameter.setJdbcType(jdbcType);
                    }

                    final String valueAttribute = valueAttribute(parameterType, parameterIndex);
                    final String formatAttribute = formatAttribute(parameterType, parameterIndex);
                    final String nameAttribute = nameAttribute(parameterType, parameterIndex);

                    if (ParameterTypes.PROCEDURE_TYPE_IN.equals(parameterType)) {
                        inParameter(parameter, attributes.get(valueAttribute), attributes.get(formatAttribute));
                        parameter.setType(ParameterType.IN);
                    } else if (ParameterTypes.PROCEDURE_TYPE_OUT.equals(parameterType)) {
                        outParameter(parameter, attributes.get(nameAttribute));
                        parameter.setType(ParameterType.OUT);
                    } else if (ParameterTypes.PROCEDURE_TYPE_INOUT.equals(parameterType)) {
                        inParameter(parameter, attributes.get(valueAttribute), attributes.get(formatAttribute));
                        outParameter(parameter, attributes.get(nameAttribute));
                        parameter.setType(ParameterType.INOUT);
                    } else {
                        //do nothing, should never be
                    }
                }
            }
        }
    }

    /**
     * Assign value and value format to parameter if the given value and format not NULL value.
     *
     * @param parameter the target parameter
     * @param value     the given input value
     * @param format    the given input value format
     */
    private static void inParameter(final Parameter parameter, final String value, final String format) {
        if (null != value) {
            parameter.setValue(value);
        }
        if (null != format) {
            parameter.setFormat(format);
        }
    }

    /**
     * Assign name for parameter if given name is not blank
     *
     * @param parameter the target parameter
     * @param name      the given name
     */
    private static void outParameter(final Parameter parameter, final String name) {
        if (StringUtils.isNotBlank(name)) {
            parameter.setName(name);
        }
    }

    private static String valueAttribute(final String parameterType, final Integer parameterIndex) {
        return String.format(AttributeConstant.PROCEDURE_VALUE_TEMPLATE, parameterType, parameterIndex);
    }

    private static String formatAttribute(final String parameterType, final Integer parameterIndex) {
        return String.format(AttributeConstant.PROCEDURE_FORMAT_TEMPLATE, parameterType, parameterIndex);
    }

    private static String nameAttribute(final String parameterType, final Integer parameterIndex) {
        return String.format(AttributeConstant.PROCEDURE_NAME_TEMPLATE, parameterType, parameterIndex);
    }
}
