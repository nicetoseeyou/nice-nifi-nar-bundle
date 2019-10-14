package lab.nice.nifi.util;

import org.apache.nifi.components.AllowableValue;

public final class PropertyConstant {
    private PropertyConstant() {

    }

    public static final AllowableValue BOOLEAN_YES = new AllowableValue("true", "true");
    public static final AllowableValue BOOLEAN_NO = new AllowableValue("false", "false");
}
