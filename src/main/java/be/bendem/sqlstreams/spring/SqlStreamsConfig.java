package be.bendem.sqlstreams.spring;

import be.bendem.sqlstreams.Sql;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@AutoConfigureAfter(DataSourceAutoConfiguration.class)
@ConditionalOnBean(DataSource.class)
@ConditionalOnClass(Sql.class)
@ConditionalOnMissingBean(Sql.class)
public class SqlStreamsConfig {

    @Bean
    public PlatformTransactionManager sqlStreamTransactionManager(Sql sql) {
        return new SqlStreamsTransactionManager(sql);
    }

    @Bean
    public Sql transactionalSqlProxy(DataSource dataSource) {
        return SqlStreamTransactionAwareProxy.createProxy(dataSource);
    }
}
