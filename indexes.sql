ALTER TABLE transactions
ADD INDEX idx_tx_cover_analytics (
    status,
    account_id,
    amount,
    direction,
    type_id
);

ALTER TABLE transactions
ADD INDEX idx_tx_depot (
    type_id,
    direction,
    status,
    date,
    amount,
    account_id
);

ALTER TABLE transactions
ADD INDEX idx_tx_parent_cover (
    parent_transfer_id,
    direction,
    status
);

ALTER TABLE accounts ADD INDEX idx_accounts_statut (statut);

ALTER TABLE accounts
ADD INDEX idx_accounts_type_statut_amount (type, statut, amount);