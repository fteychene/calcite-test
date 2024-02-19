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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import xyz.fteychene.calcite.playground.Person;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PersonFilterableTable extends AbstractTable implements FilterableTable {

    Map<String, List<Person>> usernameIndex = readFromSource().stream()
            .map(p -> Map.entry(p.getFirstname(), List.of(p)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x,y) -> Stream.concat(x.stream(), y.stream()).toList()));
    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        var avroType = AvroRelDataType.fieldType(ReflectData.get().getSchema(Person.class), relDataTypeFactory);
        return avroType;
    }

    public List<Person> readFromSource() {
        var nioPath = Paths.get("person.json");
        try {
            var payload = Files.readString(nioPath);
            var schema = Schema.createArray(ReflectData.get().getSchema(Person.class));
            ReflectDatumReader<List<Person>> datumReader = new ReflectDatumReader<>(schema);
            var decoder = DecoderFactory.get().jsonDecoder(schema, payload);
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Enumerable<Object[]> scan(DataContext dataContext, List<RexNode> filters) {

        var datas = filters.stream()
                .filter(filter -> filter.isA(SqlKind.EQUALS))
                .map(filter -> (RexCall) filter)
                .filter(rexCall -> rexCall.getOperands().get(0) instanceof RexInputRef)
                .filter(rexCall -> getRowType(dataContext.getTypeFactory()).getFieldList().get(((RexInputRef)rexCall.getOperands().get(0)).getIndex()).getName().equals("FIRSTNAME"))
                .filter(rexCall -> rexCall.getOperands().get(1) instanceof RexLiteral)
                .findFirst()
                .map(rexCall -> {
                    filters.remove(rexCall);
                    return usernameIndex.get(((RexLiteral)rexCall.getOperands().get(1)).getValueAs(String.class));
                })
                .orElseGet(this::readFromSource);
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                var schema = ReflectData.get().getSchema(Person.class);
                var fields = schema.getFields().stream()
                        .sorted(Comparator.comparing(Schema.Field::pos))
                        .toList();

                return Linq4j.enumerator(datas.stream()
                        .map(p -> fields.stream().map(field -> ReflectData.get().getField(p, field.name(), field.pos())).toArray())
                        .toList());
            }
        };
    }
}
