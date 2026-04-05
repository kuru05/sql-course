DROP PROCEDURE IF EXISTS virement_securise;

DELIMITER $$

CREATE PROCEDURE virement_securise(
    IN  p_compte_source_id  INT UNSIGNED,
    IN  p_compte_dest_id    INT UNSIGNED,
    IN  p_montant           DECIMAL(15,2),
    IN  p_devise            VARCHAR(3)
)
BEGIN
    DECLARE v_solde_source   DECIMAL(20,2);
    DECLARE v_statut_source  VARCHAR(50);
    DECLARE v_statut_dest    VARCHAR(50);
    DECLARE v_customer_id    INT UNSIGNED;
    DECLARE v_tx_debit_id    INT UNSIGNED;
    DECLARE v_tx_credit_id   INT UNSIGNED;
    DECLARE v_audit_id       INT UNSIGNED;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    IF p_montant <= 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Le montant du virement doit être strictement positif.';
    END IF;

    IF p_compte_source_id = p_compte_dest_id THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Le compte source et le compte destinataire doivent être différents.';
    END IF;

    START TRANSACTION;

    IF p_compte_source_id < p_compte_dest_id THEN
        SELECT statut, amount INTO v_statut_source, v_solde_source
        FROM accounts WHERE id = p_compte_source_id FOR UPDATE;
        SELECT statut INTO v_statut_dest
        FROM accounts WHERE id = p_compte_dest_id FOR UPDATE;
    ELSE
        SELECT statut INTO v_statut_dest
        FROM accounts WHERE id = p_compte_dest_id FOR UPDATE;
        SELECT statut, amount INTO v_statut_source, v_solde_source
        FROM accounts WHERE id = p_compte_source_id FOR UPDATE;
    END IF;

    IF v_statut_source IS NULL THEN
        SIGNAL SQLSTATE '45001' SET MESSAGE_TEXT = 'Compte source introuvable.';
    END IF;
    IF v_statut_dest IS NULL THEN
        SIGNAL SQLSTATE '45002' SET MESSAGE_TEXT = 'Compte destinataire introuvable.';
    END IF;
    IF v_statut_source != 'active' THEN
        SIGNAL SQLSTATE '45003' SET MESSAGE_TEXT = 'Compte source non actif (bloqué ou fermé). Virement refusé.';
    END IF;
    IF v_statut_dest != 'active' THEN
        SIGNAL SQLSTATE '45004' SET MESSAGE_TEXT = 'Compte destinataire non actif (bloqué ou fermé). Virement refusé.';
    END IF;
    IF v_solde_source < p_montant THEN
        SIGNAL SQLSTATE '45005' SET MESSAGE_TEXT = 'Solde insuffisant pour effectuer ce virement.';
    END IF;

    UPDATE accounts SET amount = amount - p_montant WHERE id = p_compte_source_id;
    UPDATE accounts SET amount = amount + p_montant WHERE id = p_compte_dest_id;

    INSERT INTO transactions
        (amount, date, direction, status, parent_transfer_id, devise, account_id, type_id, payment_method_id)
    VALUES (p_montant, NOW(), 'D', 'completed', NULL, p_devise, p_compte_source_id, 1, 2);
    SET v_tx_debit_id = LAST_INSERT_ID();

    INSERT INTO transactions
        (amount, date, direction, status, parent_transfer_id, devise, account_id, type_id, payment_method_id)
    VALUES (p_montant, NOW(), 'C', 'completed', v_tx_debit_id, p_devise, p_compte_dest_id, 1, 2);
    SET v_tx_credit_id = LAST_INSERT_ID();

    INSERT INTO audit_logs (time, operation, table_name, transaction_id)
    VALUES (
        NOW(),
        CONCAT('VIREMENT ', p_montant, ' ', p_devise,
               ' | source:#', p_compte_source_id, ' → dest:#', p_compte_dest_id,
               ' | tx_debit:#', v_tx_debit_id, ' tx_credit:#', v_tx_credit_id),
        'transactions', v_tx_debit_id
    );
    SET v_audit_id = LAST_INSERT_ID();

    SELECT customer_id INTO v_customer_id FROM accounts WHERE id = p_compte_source_id;
    INSERT INTO logs (customer_id, audit_log_id) VALUES (v_customer_id, v_audit_id);

    COMMIT;

    SELECT
        'Virement effectué avec succès' AS statut,
        p_montant AS montant, p_devise AS devise,
        p_compte_source_id AS compte_source_id, p_compte_dest_id AS compte_dest_id,
        v_tx_debit_id AS transaction_debit_id, v_tx_credit_id AS transaction_credit_id,
        v_audit_id AS audit_log_id;

END$$

DELIMITER;

SELECT id, num_acc, type, statut, ROUND(amount, 2) AS solde
FROM accounts
WHERE
    id IN (1, 2);

CALL virement_securise (1, 2, 500.00, 'EUR');

SELECT id, num_acc, type, statut, ROUND(amount, 2) AS solde
FROM accounts
WHERE
    id IN (1, 2);

SELECT t.id, t.direction, t.amount, t.status, t.parent_transfer_id, t.account_id
FROM transactions t
ORDER BY t.id DESC
LIMIT 2;

SELECT * FROM audit_logs ORDER BY id DESC LIMIT 1;

SELECT ROUND(amount, 2) AS solde_avant FROM accounts WHERE id = 3;

CALL virement_securise (3, 4, 99999.00, 'EUR');

SELECT ROUND(amount, 2) AS solde_apres FROM accounts WHERE id = 3;

UPDATE accounts SET statut = 'blocked' WHERE id = 5;

CALL virement_securise (1, 5, 100.00, 'EUR');

UPDATE accounts SET statut = 'active' WHERE id = 5;

CALL virement_securise (1, 2, -50.00, 'EUR');

CALL virement_securise (1, 1, 100.00, 'EUR');

START TRANSACTION;

SELECT amount FROM accounts WHERE id = 1 FOR UPDATE;

SELECT amount FROM accounts WHERE id = 2 FOR UPDATE;

UPDATE accounts SET amount = amount - 200 WHERE id = 1;

UPDATE accounts SET amount = amount + 200 WHERE id = 2;

COMMIT;

START TRANSACTION;

SELECT amount FROM accounts WHERE id = 1 FOR UPDATE;

SELECT amount FROM accounts WHERE id = 2 FOR UPDATE;

UPDATE accounts SET amount = amount - 300 WHERE id = 2;

UPDATE accounts SET amount = amount + 300 WHERE id = 1;

COMMIT;

SELECT @@transaction_isolation AS niveau_isolation_courant;

START TRANSACTION;

SELECT id, num_acc, ROUND(amount, 2) AS solde
FROM accounts
ORDER BY amount DESC;

COMMIT;