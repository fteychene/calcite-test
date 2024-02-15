package xyz.fteychene.calcite.playground;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;

import java.util.ArrayList;
import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@With
@Data
public class PersonList {

    List<Person> values = new ArrayList<>();
}
