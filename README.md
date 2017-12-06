# Sql streams SpringBoot integration

This is a spring-boot integration for the [sql-streams](https://github.com/bendem/sql-streams) project.

If you drop this dependency on your classpath at runtime, it'll pick up the configured `DataSource` and provide a transactional `Sql` instance to your application.

There is no configuration at this point, note that trying to run a query outside a transaction will fail (subject to change).
