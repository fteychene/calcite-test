package xyz.fteychene.calcite.playground.calcite;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

public class CustomSchema extends AbstractSchema {

    @Override
    protected Map<String, Table> getTableMap() {
        return Map.of("PERSONS", new PersonTable());
    }
}
