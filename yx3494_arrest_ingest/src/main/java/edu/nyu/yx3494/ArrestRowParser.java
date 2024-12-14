package edu.nyu.yx3494;

import java.util.ArrayList;
import java.util.List;

public class ArrestRowParser {
    public static String[] parse(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean inField = false;
        int fieldIndex = 0;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '\"') {
                inField = !inField; // Toggle the inField flag
            } else if (c == ',') {
                if (!inField){
                    // End of a field
                    fields.add(field.toString());
                    field.setLength(0);
                    fieldIndex++;
                } else {
                    field.append(";");
                }
            } else {
                field.append(c);
            }
        }

        // Add the last field
        fields.add(field.toString());
        fieldIndex++;

        // Check if the number of fields is correct
        if (fieldIndex != 19) {
            throw new RuntimeException("Invalid record: " + line);
        }

        return fields.toArray(new String[0]);
    }
}
