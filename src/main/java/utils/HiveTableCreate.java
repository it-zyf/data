package utils;




import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveTableCreate {

    private static String camelCase(String field) {
        final String line = String.valueOf(field.charAt(0)).toUpperCase().concat(field.substring(1));
        final Matcher matcher = Pattern.compile("[A-Z]([a-z\\d]+)?").matcher(line);
        final StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String word = matcher.group();
            sb.append(word.toUpperCase());
            sb.append(matcher.end() == line.length() ? "" : "_");
        }
        return sb.toString().toLowerCase();
    }

    public static StringBuilder parser(Class aClass) {
        final StringBuilder builder = new StringBuilder();
        final Class superclass = aClass.getSuperclass();
        if (!"Object".equalsIgnoreCase(superclass.getSimpleName())) {
            builder.append(parser(superclass));
        }
        final Field[] fields = aClass.getDeclaredFields();
        for (Field field : fields) {
            if (Modifier.PRIVATE == field.getModifiers()) {
                final String type = field.getType().getSimpleName().toLowerCase();
                final String name = camelCase(field.getName());
                builder.append(",\n");
                switch (type) {
                    case "boolean":
                        builder.append(name).append(" ").append("boolean");
                        break;
                    case "integer":
                        builder.append(name).append(" ").append("int");
                        break;
                    case "date":
                    case "long":
                        builder.append(name).append(" ").append("bigint");
                        break;
                    case "float":
                        builder.append(name).append(" ").append("float");
                        break;
                    case "double":
                        builder.append(name).append(" ").append("double");
                        break;
                    default:
                        builder.append(name).append(" ").append("string");
                        break;
                }
            }
        }
        return builder;
    }
}
