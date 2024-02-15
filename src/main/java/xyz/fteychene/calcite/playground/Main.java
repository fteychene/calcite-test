package xyz.fteychene.calcite.playground;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;

@Slf4j
public class Main {

    public static void main(String[] args) throws IOException, SQLException {
        var nioPath = Paths.get("data.json");
        if (!nioPath.toFile().exists()) {
            throw new RuntimeException("data.json doesn't exist");
        }

        Connection connection = null;
        Statement statement = null;
        try {
            Properties info = new Properties();
            info.put("model", "/home/fteychene/projects/calcite-tutorial/model,json");
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            statement = connection.createStatement();
            final ResultSet resultSet =
                    statement.executeQuery("SELECT * FROM persons p  WHERE p.age > 45 AND p.firstname LIKE 'Je%' AND p.ranking < 30.0 AND CARDINALITY(p.departments) > 0 ");
            log.info("Columns : {}", resultSet.getMetaData().getColumnCount());
            log.info("{} | {} | {} | {} | {} | {}", resultSet.getMetaData().getColumnName(1), resultSet.getMetaData().getColumnName(2), resultSet.getMetaData().getColumnName(3), resultSet.getMetaData().getColumnName(4), resultSet.getMetaData().getColumnName(5), resultSet.getMetaData().getColumnName(6));
            while (resultSet.next()) {
                log.info("{} | {} | {} | {} | {} | {}", resultSet.getInt(1), resultSet.getArray(2), resultSet.getString(3), resultSet.getString(4), resultSet.getDouble(5), resultSet.getString(6));
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
