package xyz.fteychene.calcite.playground;

import com.github.javafaker.Faker;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.checkerframework.common.value.qual.IntRange;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.stream.IntStream;

@Slf4j
public class DataGenerator {

    public static void main(String[] args) throws IOException {
        var nioPath = Paths.get("data.json");
        if (nioPath.toFile().exists()) {
            Files.delete(nioPath);
        }

        try (var writer = new FileOutputStream(nioPath.toFile())) {
            ReflectDatumWriter<PersonList> datumWriter = new ReflectDatumWriter<>(PersonList.class);
            var encoder = EncoderFactory.get().jsonEncoder(ReflectData.get().getSchema(PersonList.class), writer, true);
            var persons = IntStream.range(0, 2000)
                    .boxed()
                    .map(index -> {
                        var faker = new Faker(new Random(index));
                        var name = faker.name();
                        return new Person(
                                name.username(),
                                name.firstName(),
                                name.lastName(),
                                faker.random().nextInt(18, 78),
                                faker.random().nextDouble() * 100,
                                IntStream.range(0, faker.random().nextInt(3)).boxed().map(__ -> faker.job().title()).toList()
                        );
                    }).toList();
            datumWriter.write(new PersonList(persons), encoder);
            encoder.flush();
        }
    }
}
