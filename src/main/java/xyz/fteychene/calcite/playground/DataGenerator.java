package xyz.fteychene.calcite.playground;

import com.github.javafaker.Faker;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

@Slf4j
public class DataGenerator {

    public static void main(String[] args) throws IOException {
        var personPath = Paths.get("person.json");
        if (personPath.toFile().exists()) {
            Files.delete(personPath);
        }
        var teamPath = Paths.get("team.json");
        if (teamPath.toFile().exists()) {
            Files.delete(teamPath);
        }

        try (var personFileOut = new FileOutputStream(personPath.toFile());
             var teamFileOut = new FileOutputStream(teamPath.toFile());) {
            var personList = Schema.createArray(ReflectData.get().getSchema(Person.class));
            var teamList = Schema.createArray(ReflectData.get().getSchema(Team.class));
            DatumWriter<List<Person>> personAvroWriter = new ReflectDatumWriter<>(personList);
            DatumWriter<List<Team>> teamAvroWriter = new ReflectDatumWriter<>(teamList);
            var personListEncoder = EncoderFactory.get().jsonEncoder(personList, personFileOut, true);
            var teamListEncoder = EncoderFactory.get().jsonEncoder(teamList, teamFileOut, true);

            var teams = IntStream.range(0, 40)
                    .boxed()
                    .map(index -> {
                        var team = new Faker(new Random(index)).team();
                        return new Team(
                                UUID.randomUUID().toString(),
                                team.name(),
                                team.creature(),
                                team.state(),
                                team.sport()
                        );
                    }).toList();

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
                                IntStream.range(0, faker.random().nextInt(3)).boxed().map(__ -> faker.job().title()).toList(),
                                teams.get(faker.random().nextInt(teams.size())).getId()
                        );
                    }).toList();

            teamAvroWriter.write(teams, teamListEncoder);
            personAvroWriter.write(persons, personListEncoder);
            teamListEncoder.flush();
            personListEncoder.flush();
        }
    }
}
