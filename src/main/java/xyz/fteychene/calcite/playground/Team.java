package xyz.fteychene.calcite.playground;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;

import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@With
public class Team {
    String id;
    String name;
    String creature;
    String state;
    String sport;
}
