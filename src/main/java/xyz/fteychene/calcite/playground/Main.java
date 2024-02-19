package xyz.fteychene.calcite.playground;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class Main {

    public static void main(String[] args) throws IOException, SQLException {
        var nioPath = Paths.get("person.json");
        if (!nioPath.toFile().exists()) {
            throw new RuntimeException("person.json doesn't exist");
        }

        Connection connection = null;
        Statement statement = null;
        try {
            Properties info = new Properties();
            info.put("model", "/home/fteychene/projects/calcite-tutorial/model,json");
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            statement = connection.createStatement();
            final ResultSet resultSet =
                    statement.executeQuery("SELECT * FROM person p where p.firstname = 'Paul'");
//                    WHERE p.age > 45 AND p.firstname LIKE 'Je%' AND p.ranking < 30.0 AND CARDINALITY(p.departments) > 0
//                    statement.executeQuery("SELECT * FROM person p inner join team t on p.teamId = t.id");
//                    statement.executeQuery("SELECT * FROM team t");
//                    statement.executeQuery("SELECT t.id, t.name, COUNT(p.username) as employees FROM person p inner join team t on p.teamId = t.id GROUP BY t.id, t.name HAVING COUNT(p.username) > 60");

            log.info("Columns : {}", resultSet.getMetaData().getColumnCount());
            log.info(IntStream.range(1, resultSet.getMetaData().getColumnCount()+1).boxed()
                    .map(index -> {
                        try {
                            return resultSet.getMetaData().getTableName(index) + "."+ resultSet.getMetaData().getColumnName(index);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.joining(" | ")));
          while (resultSet.next()) {
              log.info(IntStream.range(1, resultSet.getMetaData().getColumnCount()+1).boxed()
                      .map(index -> {
                          try {
                              return resultSet.getString(index);
                          } catch (SQLException e) {
                              throw new RuntimeException(e);
                          }
                      })
                      .collect(Collectors.joining(" | ")));
//                log.info("{} | {} | {} | {} | {} | {} | {}", resultSet.getInt(1), resultSet.getArray(2), resultSet.getString(3), resultSet.getString(4), resultSet.getDouble(5), resultSet.getString(6), resultSet.getString(7));
            }
        } finally {
            close(connection, statement);
        }
    }

    private static void close(Connection connection, Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }
}
