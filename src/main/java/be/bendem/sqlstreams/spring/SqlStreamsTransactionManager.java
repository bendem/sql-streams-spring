package be.bendem.sqlstreams.spring;

import be.bendem.sqlstreams.Sql;
import be.bendem.sqlstreams.Transaction;
import be.bendem.sqlstreams.util.Wrap;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;

public class SqlStreamsTransactionManager implements PlatformTransactionManager {

    private static Transaction.IsolationLevel translateIsolationLevel(int isolationLevel) {
        switch (isolationLevel) {
            case TransactionDefinition.ISOLATION_READ_COMMITTED: return Transaction.IsolationLevel.READ_COMMITTED;
            case TransactionDefinition.ISOLATION_READ_UNCOMMITTED: return Transaction.IsolationLevel.READ_UNCOMMITTED;
            case TransactionDefinition.ISOLATION_REPEATABLE_READ: return Transaction.IsolationLevel.REPEATABLE_READ;
            case TransactionDefinition.ISOLATION_SERIALIZABLE: return Transaction.IsolationLevel.SERIALIZABLE;
        }
        throw new IllegalArgumentException("invalid transaction isolation: " + isolationLevel);
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
        Transaction.IsolationLevel isolationLevel = translateIsolationLevel(definition.getIsolationLevel());
        Transaction t;
        if (isolationLevel == null) {
            t = sql.transaction();
        } else {
            t = sql.transaction(isolationLevel);
        }

        if (definition.isReadOnly()) {
            Wrap.execute(() -> t.getConnection().setReadOnly(true));
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
