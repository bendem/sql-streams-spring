package be.bendem.sqlstreams.spring;

import be.bendem.sqlstreams.Sql;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.interceptor.TransactionalProxy;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class SqlStreamTransactionAwareProxy implements InvocationHandler {

    // TODO Make this configurable?
    private static final boolean FAIL_WITHOUT_TRANSACTION = true;

    interface SqlStreamHolder {
        Sql sql();
    }

    public static Sql createProxy(DataSource dataSource) {
        return (Sql) Proxy.newProxyInstance(
            SqlStreamTransactionStatus.class.getClassLoader(),
            new Class<?>[] {
                Sql.class,
                SqlStreamHolder.class,
                TransactionalProxy.class // I have no idea if this is correct, but meh
            },
            new SqlStreamTransactionAwareProxy(dataSource)
        );
    }

    private final Sql sql;

    public SqlStreamTransactionAwareProxy(DataSource dataSource) {
        this.sql = Sql.connect(dataSource);
    }

    @Override
    public Object invoke(Object o, Method method, Object[] objects) throws Throwable {
        Sql sql = this.sql;

        if (method.getDeclaringClass() == SqlStreamHolder.class) {
            return sql;
        }

        TransactionStatus transactionStatus = null;
        try {
            transactionStatus = TransactionAspectSupport.currentTransactionStatus();
        } catch (NoTransactionException e) {
            if (FAIL_WITHOUT_TRANSACTION) {
                throw e;
            }
        }
        if (transactionStatus instanceof SqlStreamTransactionStatus) {
            sql = ((SqlStreamTransactionStatus) transactionStatus).transaction;
        }

        return method.invoke(sql, objects);
    }
}
