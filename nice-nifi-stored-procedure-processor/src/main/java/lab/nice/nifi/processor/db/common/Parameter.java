package lab.nice.nifi.processor.db.common;

import java.sql.JDBCType;
import java.util.Objects;

public class Parameter {
    private ParameterType type;
    private final Integer index;
    private JDBCType jdbcType;
    private String value;
    private String format;
    private String name;

    public Parameter(final String type, final Integer index, final Integer jdbcType) {
        this.type = ParameterType.from(type);
        this.index = index;
        this.jdbcType = JDBCType.valueOf(jdbcType);
    }

    public ParameterType getType() {
        return type;
    }

    public void setType(final ParameterType type) {
        this.type = type;
    }

    public Integer getIndex() {
        return index;
    }

    public JDBCType getJdbcType() {
        return jdbcType;
    }

    public void setJdbcType(final Integer jdbcType) {
        if (this.jdbcType.getVendorTypeNumber() != jdbcType){
            this.jdbcType = JDBCType.valueOf(jdbcType);
        }
    }

    public String getValue() {
        return value;
    }

    public void setValue(final String value) {
        this.value = value;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(final String format) {
        this.format = format;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Parameter parameter = (Parameter) o;
        return type == parameter.type &&
                Objects.equals(index, parameter.index) &&
                jdbcType == parameter.jdbcType &&
                Objects.equals(value, parameter.value) &&
                Objects.equals(format, parameter.format) &&
                Objects.equals(name, parameter.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, index, jdbcType, value, format, name);
    }

    @Override
    public String toString() {
        return "Parameter{" +
                "type=" + type +
                ", index=" + index +
                ", jdbcType=" + jdbcType +
                ", value='" + value + '\'' +
                ", format='" + format + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
