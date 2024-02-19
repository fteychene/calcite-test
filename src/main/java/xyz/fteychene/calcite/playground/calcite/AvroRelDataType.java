package xyz.fteychene.calcite.playground.calcite;

import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Comparator;
import java.util.Map;

public class AvroRelDataType {
    
    public static RelDataType fieldType(Schema avroSchema, RelDataTypeFactory typeFactory) {
        var result = typeFactory.builder();
        avroSchema.getFields().stream()
                .sorted(Comparator.comparing(Schema.Field::pos))
                .map(field -> Map.entry(field.name().toUpperCase(), switch (field.schema().getType()) {
                    case RECORD -> throw new UnsupportedOperationException("Don't support record type for now");
                    case ENUM, STRING ->
                            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), field.schema().isNullable());
                    case ARRAY ->
                            typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.VARCHAR), -1);
                    case MAP -> throw new UnsupportedOperationException("Don't support map type for now");
                    case UNION -> throw new UnsupportedOperationException("Don't support union type for now");
                    case FIXED ->
                            typeFactory.createSqlType(SqlTypeName.VARBINARY, field.schema().getFixedSize());
                    case BYTES ->
                            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BINARY), field.schema().isNullable());
                    case INT ->
                            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), field.schema().isNullable());
                    case LONG ->
                            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), field.schema().isNullable());
                    case FLOAT ->
                            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DECIMAL), field.schema().isNullable());
                    case DOUBLE ->
                            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), field.schema().isNullable());
                    case BOOLEAN ->
                            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN), field.schema().isNullable());
                    case NULL ->
                            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.NULL), field.schema().isNullable());
                }))
                .forEach(relDataType -> result.add(relDataType.getKey(), relDataType.getValue()));
        return result
                .build();
    }
}
