package xyz.fteychene.calcite.playground.calcite;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import xyz.fteychene.calcite.playground.Team;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class TeamTable extends AbstractTable implements ScannableTable {
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        var nioPath = Paths.get("team.json");
        try {
            var payload = Files.readString(nioPath);
            var schema = Schema.createArray(ReflectData.get().getSchema(Team.class));
            ReflectDatumReader<List<Team>> datumReader = new ReflectDatumReader<>(schema);
            var decoder = DecoderFactory.get().jsonDecoder(schema, payload);
            var teams = datumReader.read(null, decoder);
            return new AbstractEnumerable<>() {
                @Override
                public Enumerator<Object[]> enumerator() {
                    var schema = ReflectData.get().getSchema(Team.class);
                    var fields = schema.getFields().stream()
                            .sorted(Comparator.comparing(Schema.Field::pos))
                            .toList();

                    return Linq4j.enumerator(teams.stream()
                            .map(p -> fields.stream().map(field -> ReflectData.get().getField(p, field.name(), field.pos())).toArray())
                            .toList());
                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        var avroSchema = ReflectData.get().getSchema(Team.class);
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
