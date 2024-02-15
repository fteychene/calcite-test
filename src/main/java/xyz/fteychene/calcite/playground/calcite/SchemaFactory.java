package xyz.fteychene.calcite.playground.calcite;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class SchemaFactory implements org.apache.calcite.schema.SchemaFactory {
    @Override
    public Schema create(SchemaPlus schemaPlus, String s, Map<String, Object> map) {
        return new CustomSchema();
    }
}
