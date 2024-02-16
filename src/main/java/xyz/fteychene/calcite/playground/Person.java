package xyz.fteychene.calcite.playground;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.time.Instant;
import java.util.List;

@Data
@With
@AllArgsConstructor
@NoArgsConstructor
public class Person {

    String username;
    String firstname;
    String lastname;
    Integer age;
    Double ranking;
    List<String> departments;
    String teamId;

}
