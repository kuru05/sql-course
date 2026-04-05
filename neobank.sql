-- Transaction Types --
CREATE TABLE transaction_types (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    label VARCHAR(50) NOT NULL UNIQUE
);

-- Payment Methods --
CREATE TABLE payment_methods (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    label VARCHAR(50) NOT NULL UNIQUE
);

-- Customers --
CREATE TABLE customers (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    email VARCHAR(70) NOT NULL UNIQUE,
    phone VARCHAR(20) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Beneficiaries --
CREATE TABLE beneficiaries (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    iban VARCHAR(35) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Registered As --
CREATE TABLE registered_as (
    customer_id INT UNSIGNED NOT NULL,
    beneficiary_id INT UNSIGNED NOT NULL,
    PRIMARY KEY (customer_id, beneficiary_id),
    FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE CASCADE,
    FOREIGN KEY (beneficiary_id) REFERENCES beneficiaries (id) ON DELETE CASCADE
);

-- Accounts --
CREATE TABLE accounts (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    num_acc VARCHAR(34) NOT NULL UNIQUE,
    type VARCHAR(50) NOT NULL,
    amount DECIMAL(20, 2) NOT NULL DEFAULT 0.00 CHECK (amount >= 0),
    devise VARCHAR(10) NOT NULL DEFAULT 'EUR',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    statut VARCHAR(50) NOT NULL DEFAULT 'active' CHECK (
        statut IN ('active', 'blocked', 'closed')
    ),
    customer_id INT UNSIGNED NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE RESTRICT
);

-- Card Types --
CREATE TABLE card_types (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    label VARCHAR(50) NOT NULL UNIQUE
);

-- Card Networks --
CREATE TABLE card_networks (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    label VARCHAR(50) NOT NULL UNIQUE
);

-- Cards --
CREATE TABLE cards (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    card_number VARCHAR(16) NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL DEFAULT 'active' CHECK (
        status IN (
            'active',
            'blocked',
            'expired',
            'cancelled'
        )
    ),
    payment_limit DECIMAL(10, 2) NOT NULL CHECK (payment_limit > 0),
    withdrawal_limit DECIMAL(10, 2) NOT NULL CHECK (withdrawal_limit > 0),
    expiry_date DATE NOT NULL,
    account_id INT UNSIGNED NOT NULL,
    card_type_id INT UNSIGNED NOT NULL,
    card_network_id INT UNSIGNED NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (id) ON DELETE RESTRICT,
    FOREIGN KEY (card_type_id) REFERENCES card_types (id) ON DELETE RESTRICT,
    FOREIGN KEY (card_network_id) REFERENCES card_networks (id) ON DELETE RESTRICT
);

-- Transactions --
CREATE TABLE transactions (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    amount DECIMAL(15, 2) NOT NULL CHECK (amount > 0),
    date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    direction VARCHAR(1) NOT NULL CHECK (direction IN ('D', 'C')),
    status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (
        status IN (
            'pending',
            'completed',
            'failed',
            'cancelled'
        )
    ),
    parent_transfer_id INT UNSIGNED,
    devise VARCHAR(3) NOT NULL DEFAULT 'EUR',
    account_id INT UNSIGNED NOT NULL,
    type_id INT UNSIGNED NOT NULL,
    payment_method_id INT UNSIGNED NOT NULL,
    card_id INT UNSIGNED,
    FOREIGN KEY (account_id) REFERENCES accounts (id) ON DELETE RESTRICT,
    FOREIGN KEY (type_id) REFERENCES transaction_types (id) ON DELETE RESTRICT,
    FOREIGN KEY (payment_method_id) REFERENCES payment_methods (id) ON DELETE RESTRICT,
    FOREIGN KEY (parent_transfer_id) REFERENCES transactions (id) ON DELETE SET NULL,
    FOREIGN KEY (card_id) REFERENCES cards (id) ON DELETE SET NULL
);

-- Audit Logs --
CREATE TABLE audit_logs (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    operation VARCHAR(100),
    table_name VARCHAR(50),
    transaction_id INT UNSIGNED,
    FOREIGN KEY (transaction_id) REFERENCES transactions (id) ON DELETE SET NULL
);

-- Logs --
CREATE TABLE logs (
    customer_id INT UNSIGNED NOT NULL,
    audit_log_id INT UNSIGNED NOT NULL,
    PRIMARY KEY (customer_id, audit_log_id),
    FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE CASCADE,
    FOREIGN KEY (audit_log_id) REFERENCES audit_logs (id) ON DELETE CASCADE
);

-- Indexes --
CREATE INDEX idx_accounts_customer_id ON accounts (customer_id);

CREATE INDEX idx_transactions_account_id ON transactions (account_id);

CREATE INDEX idx_transactions_date ON transactions (date);

CREATE INDEX idx_transactions_status ON transactions (status);

CREATE INDEX idx_transactions_type_id ON transactions (type_id);

CREATE INDEX idx_audit_logs_time ON audit_logs (time);

CREATE INDEX idx_cards_account_id ON cards (account_id);