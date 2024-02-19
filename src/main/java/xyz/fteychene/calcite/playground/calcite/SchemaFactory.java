package xyz.fteychene.calcite.playground.calcite;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

public class SchemaFactory implements org.apache.calcite.schema.SchemaFactory {
    @Override
    public Schema create(SchemaPlus schemaPlus, String s, Map<String, Object> map) {
        return new AbstractSchema() {
            @Override
            protected Map<String, Table> getTableMap() {
                return Map.of(
                        "PERSON", new PersonFilterableTable(),
                        "TEAM", new TeamTable()
                );
            }
        };
    }
}
