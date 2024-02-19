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
import xyz.fteychene.calcite.playground.Person;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;

public class PersonTable extends AbstractTable implements ScannableTable {
    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
       return AvroRelDataType.fieldType(ReflectData.get().getSchema(Person.class), relDataTypeFactory);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext dataContext) {
        var nioPath = Paths.get("person.json");
        try {
            var payload = Files.readString(nioPath);
            var schema = Schema.createArray(ReflectData.get().getSchema(Person.class));
            ReflectDatumReader<List<Person>> datumReader = new ReflectDatumReader<>(schema);
            var decoder = DecoderFactory.get().jsonDecoder(schema, payload);
            var persons = datumReader.read(null, decoder);
            return new AbstractEnumerable<>() {
                @Override
                public Enumerator<Object[]> enumerator() {
                    var schema = ReflectData.get().getSchema(Person.class);
                    var fields = schema.getFields().stream()
                            .sorted(Comparator.comparing(Schema.Field::pos))
                            .toList();

                    return Linq4j.enumerator(persons.stream()
                            .map(p -> fields.stream().map(field -> ReflectData.get().getField(p, field.name(), field.pos())).toArray())
                            .toList());
                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
