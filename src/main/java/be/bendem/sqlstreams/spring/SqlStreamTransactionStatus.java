package be.bendem.sqlstreams.spring;

import be.bendem.sqlstreams.Transaction;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;

import java.sql.SQLException;
import java.sql.Savepoint;

public class SqlStreamTransactionStatus implements TransactionStatus {

    final Transaction transaction;
    private boolean rollbackOnly = false;
    private boolean closed = false;

    public SqlStreamTransactionStatus(Transaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public boolean isNewTransaction() {
        return false;
    }

    @Override
    public boolean hasSavepoint() {
        return false;
    }

    @Override
    public void setRollbackOnly() {
        rollbackOnly = true;
    }

    @Override
    public boolean isRollbackOnly() {
        return rollbackOnly;
    }

    @Override
    public void flush() {
    }

    @Override
    public boolean isCompleted() {
        return closed;
    }

    @Override
    public Object createSavepoint() throws TransactionException {
        try {
            return transaction.getConnection().setSavepoint();
        } catch (SQLException e) {
            throw new TransactionSystemException(e.getMessage(), e);
        }
    }

    @Override
    public void rollbackToSavepoint(Object savepoint) throws TransactionException {
        try {
            transaction.getConnection().rollback((Savepoint) savepoint);
        } catch (SQLException e) {
            throw new TransactionSystemException(e.getMessage(), e);
        }
    }

    @Override
    public void releaseSavepoint(Object savepoint) throws TransactionException {
        try {
            transaction.getConnection().releaseSavepoint((Savepoint) savepoint);
        } catch (SQLException e) {
            throw new TransactionSystemException(e.getMessage(), e);
        }
    }

    void commit() {
        transaction.commit();
        close();
    }

    void rollback() {
        transaction.rollback();
        close();
    }

    private void close() {
        closed = true;
        transaction.close();
    }
}
