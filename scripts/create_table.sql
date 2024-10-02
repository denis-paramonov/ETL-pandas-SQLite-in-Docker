CREATE TABLE IF NOT EXISTS transactions (
        transaction_id INTEGER,
        user_id INTEGER NOT NULL,
        transaction_amount REAL NOT NULL,
        transaction_date DATE NOT NULL,
        transaction_type TEXT NOT NULL,
        year INTEGER,
        month INTEGER,
        is_refund BOOLEAN,
        PRIMARY KEY(transaction_id)
    );
    CREATE INDEX IF NOT EXISTS idx_transaction_id ON transactions(transaction_id);
    CREATE INDEX IF NOT EXISTS idx_user_id ON transactions(user_id);