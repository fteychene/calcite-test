package xyz.fteychene.calcite.playground;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteMetaImpl;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.QueryProviderImpl;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.tools.RelRunner;
import org.checkerframework.checker.nullness.qual.Nullable;
import xyz.fteychene.calcite.playground.calcite.PersonFilterableTable;
import xyz.fteychene.calcite.playground.calcite.SchemaFactory;
import xyz.fteychene.calcite.playground.calcite.TeamTable;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.StreamSupport;

@Slf4j
public class NoJdbcMain {

    public static void main(String[] args) {
        var prepare = new CalcitePrepareImpl();
        var dataContext = new CodeContext();
        var requestsignature = prepare.prepareSql(dataContext, CalcitePrepare.Query.of("SELECT * FROM person p where p.firstname = 'Paul'"), Object[].class, -1);
        log.info("COUCOUUUUUUUUUUUU");
        dataContext.context.datas.putAll(requestsignature.internalParameters);
        var result = requestsignature.enumerable(dataContext.getDataContext());
        log.info("Columns : {}", requestsignature.columns.stream().map(c -> c.columnName).toList());
        StreamSupport.stream(result.spliterator(), false)
                .forEach(x -> log.info("Value : {}", x));

    }

    static class CodeContext implements CalcitePrepare.Context {
        CalciteSchema schema = CalciteSchema.createRootSchema(true);
        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
        CodeDataContext context;

        public CodeContext() {
            schema.add("test", new AbstractSchema() {
                @Override
                protected Map<String, Table> getTableMap() {
                    return Map.of(
                            "PERSON", new PersonFilterableTable(),
                            "TEAM", new TeamTable()
                    );
                }
            });
            context = new CodeDataContext(schema.plus(), typeFactory);
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return typeFactory;
        }

        @Override
        public CalciteSchema getRootSchema() {
            return schema;
        }

        @Override
        public CalciteSchema getMutableRootSchema() {
            return schema;
        }

        @Override
        public List<String> getDefaultSchemaPath() {
            return List.of("test");
        }

        @Override
        public CalciteConnectionConfig config() {
            return new CalciteConnectionConfigImpl(new Properties());
        }

        @Override
        public CalcitePrepare.SparkHandler spark() {
            return new CalcitePrepare.SparkHandler() {
                @Override
                public RelNode flattenTypes(RelOptPlanner planner, RelNode rootRel, boolean restructure) {
                    return null;
                }

                @Override
                public void registerRules(RuleSetBuilder builder) {

                }

                @Override
                public boolean enabled() {
                    return false;
                }

                @Override
                public ArrayBindable compile(ClassDeclaration expr, String s) {
                    return null;
                }

                @Override
                public Object sparkContext() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public DataContext getDataContext() {
            return context;
        }

        @Override
        public @Nullable List<String> getObjectPath() {
            return null;
        }

        @Override
        public RelRunner getRelRunner() {
            return new RelRunner() {
                @Override
                public PreparedStatement prepareStatement(RelNode rel) throws SQLException {
                    throw new UnsupportedOperationException("RelRunner.prepareStatement");
                }
            };
        }
    }

    static class CodeDataContext implements DataContext {

        public SchemaPlus schema;
        public JavaTypeFactory typeFactory;
        public Map<String, Object> datas = new HashMap<>();

        public CodeDataContext(SchemaPlus schema, JavaTypeFactory typeFactory) {
            this.schema = schema;
            this.typeFactory = typeFactory;
        }

        @Override
        public @Nullable SchemaPlus getRootSchema() {
            return schema;
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return typeFactory;
        }

        @Override
        public QueryProvider getQueryProvider() {
            return new QueryProviderImpl() {
                @Override
                public <T> Enumerator<T> executeQuery(Queryable<T> queryable) {
                    return queryable.enumerator();
                }
            };
        }

        @Override
        public @Nullable Object get(String name) {
            log.info("Get called for {} on data context", name);
            return datas.get(name);
        }

    }
}
