package be.bendem.sqlstreams.spring;

import be.bendem.sqlstreams.Sql;
import be.bendem.sqlstreams.Transaction;
import be.bendem.sqlstreams.util.Wrap;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SqlStreamsTransactionManager implements PlatformTransactionManager {

    private static final Map<Integer, Transaction.IsolationLevel> ISOLATION_LEVEL_MAP;
    static {
        Map<Integer, Transaction.IsolationLevel> map = new HashMap<>();
        map.put(TransactionDefinition.ISOLATION_READ_COMMITTED, Transaction.IsolationLevel.READ_COMMITTED);
        map.put(TransactionDefinition.ISOLATION_READ_UNCOMMITTED, Transaction.IsolationLevel.READ_UNCOMMITTED);
        map.put(TransactionDefinition.ISOLATION_REPEATABLE_READ, Transaction.IsolationLevel.REPEATABLE_READ);
        map.put(TransactionDefinition.ISOLATION_SERIALIZABLE, Transaction.IsolationLevel.SERIALIZABLE);

        ISOLATION_LEVEL_MAP = Collections.unmodifiableMap(map);
    }

    private final Sql sql;

    public SqlStreamsTransactionManager(Sql sql) {
        if (sql instanceof SqlStreamTransactionAwareProxy.SqlStreamHolder) {
            this.sql = ((SqlStreamTransactionAwareProxy.SqlStreamHolder) sql).sql();
        } else {
            this.sql = sql;
        }
    }

    @Override
    public TransactionStatus getTransaction(TransactionDefinition definition) throws TransactionException {
        Transaction.IsolationLevel isolationLevel = ISOLATION_LEVEL_MAP.get(definition.getIsolationLevel());
        Transaction t;
        if (isolationLevel == null) {
            t = sql.transaction();
        } else {
            t = sql.transaction(isolationLevel);

            if (definition.isReadOnly()) {
                Wrap.execute(() -> t.getConnection().setReadOnly(true));
            }
        }
        return new SqlStreamTransactionStatus(t);
    }

    @Override
    public void commit(TransactionStatus status) throws TransactionException {
        ((SqlStreamTransactionStatus) status).commit();
    }

    @Override
    public void rollback(TransactionStatus status) throws TransactionException {
        ((SqlStreamTransactionStatus) status).rollback();
    }
}
