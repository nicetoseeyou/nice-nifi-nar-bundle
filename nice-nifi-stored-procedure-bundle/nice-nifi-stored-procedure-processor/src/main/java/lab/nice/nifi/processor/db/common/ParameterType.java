package lab.nice.nifi.processor.db.common;

import org.apache.commons.lang3.StringUtils;

/**
 * Built-in stored procedure parameter type
 */
public enum ParameterType {
    IN(ParameterTypes.PROCEDURE_TYPE_IN),
    OUT(ParameterTypes.PROCEDURE_TYPE_OUT),
    INOUT(ParameterTypes.PROCEDURE_TYPE_INOUT);

    private String type;

    ParameterType(final String type) {
        this.type = type;
    }

    public String getName() {
        return type;
    }

    /**
     * Get parameter type from String type.
     *
     * @param type the type
     * @return enum parameter type
     */
    public static ParameterType from(final String type) {
        for (ParameterType parameterType : ParameterType.class.getEnumConstants()) {
            if (StringUtils.equalsIgnoreCase(parameterType.type, type)) {
                return parameterType;
            }
        }
        throw new IllegalArgumentException("Type:" + type + " is not a valid "
                + "lab.nice.nifi.invoker.common.ParameterType.java value.");
    }
}
