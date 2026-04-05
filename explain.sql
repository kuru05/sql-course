EXPLAIN FORMAT = TREE
WITH
    activite_client AS (
        SELECT
            c.id AS client_id,
            c.name AS client,
            COUNT(t.id) AS nb_transactions,
            SUM(t.amount) AS volume_total,
            SUM(
                CASE
                    WHEN t.direction = 'C'
                    AND t.type_id = 4 THEN t.amount
                    ELSE 0
                END
            ) AS total_depots,
            SUM(
                CASE
                    WHEN t.direction = 'D' THEN t.amount
                    ELSE 0
                END
            ) AS total_debits
        FROM
            customers c
            JOIN accounts a ON a.customer_id = c.id
            JOIN transactions t ON t.account_id = a.id
        WHERE
            t.status = 'completed'
        GROUP BY
            c.id,
            c.name
    ),
    classement AS (
        SELECT
            *,
            RANK() OVER (
                ORDER BY volume_total DESC
            ) AS rang_volume,
            RANK() OVER (
                ORDER BY total_depots DESC
            ) AS rang_depots,
            RANK() OVER (
                ORDER BY nb_transactions DESC
            ) AS rang_activite
        FROM activite_client
    )
SELECT
    rang_volume AS rang,
    client,
    nb_transactions,
    ROUND(volume_total, 2),
    ROUND(total_depots, 2),
    ROUND(total_debits, 2),
    rang_depots,
    rang_activite
FROM classement
WHERE
    rang_volume <= 10
ORDER BY rang_volume;

EXPLAIN FORMAT = TREE
WITH
    stats_compte AS (
        SELECT account_id, AVG(amount) AS moy_montant
        FROM transactions
        WHERE
            status = 'completed'
        GROUP BY
            account_id
    ),
    freq_journaliere AS (
        SELECT account_id, DATE(date) AS jour, COUNT(*) AS nb_jour
        FROM transactions
        WHERE
            status IN ('completed', 'pending')
        GROUP BY
            account_id,
            DATE(date)
    ),
    anomalies AS (
        SELECT
            t.id,
            t.date,
            c.name AS client,
            a.num_acc,
            a.statut,
            tt.label,
            t.amount,
            t.direction,
            t.status,
            CASE
                WHEN s.moy_montant IS NOT NULL
                AND t.amount > 3 * s.moy_montant THEN 'R1'
            END AS regle_r1,
            CASE
                WHEN a.statut = 'blocked' THEN 'R2'
            END AS regle_r2,
            CASE
                WHEN fj.nb_jour > 5 THEN 'R3'
            END AS regle_r3,
            CASE
                WHEN t.type_id = 1
                AND t.direction = 'D'
                AND t2.id IS NULL THEN 'R4'
            END AS regle_r4
        FROM
            transactions t
            JOIN accounts a ON a.id = t.account_id
            JOIN customers c ON c.id = a.customer_id
            JOIN transaction_types tt ON tt.id = t.type_id
            LEFT JOIN stats_compte s ON s.account_id = t.account_id
            LEFT JOIN freq_journaliere fj ON fj.account_id = t.account_id
            AND fj.jour = DATE(t.date)
            LEFT JOIN transactions t2 ON t2.parent_transfer_id = t.id
            AND t2.direction = 'C'
            AND t2.status = 'completed'
    )
SELECT
    id,
    date,
    client,
    num_acc,
    statut,
    label,
    amount,
    direction,
    status,
    CONCAT_WS(
        ' | ',
        regle_r1,
        regle_r2,
        regle_r3,
        regle_r4
    ) AS raisons
FROM anomalies
WHERE
    regle_r1 IS NOT NULL
    OR regle_r2 IS NOT NULL
    OR regle_r3 IS NOT NULL
    OR regle_r4 IS NOT NULL
ORDER BY date DESC;

EXPLAIN FORMAT = TREE
WITH
    depots_mensuels AS (
        SELECT
            YEAR(t.date) AS annee,
            MONTH(t.date) AS mois,
            a.type AS type_compte,
            COUNT(t.id) AS nb_depots,
            SUM(t.amount) AS montant_total,
            AVG(t.amount) AS montant_moyen,
            COUNT(DISTINCT t.account_id) AS nb_comptes_actifs
        FROM transactions t
            JOIN accounts a ON a.id = t.account_id
        WHERE
            t.type_id = 4
            AND t.direction = 'C'
            AND t.status = 'completed'
        GROUP BY
            YEAR(t.date),
            MONTH(t.date),
            a.type
    )
SELECT
    annee,
    mois,
    type_compte,
    nb_depots,
    ROUND(montant_total, 2),
    ROUND(
        SUM(montant_total) OVER (
            PARTITION BY
                type_compte
            ORDER BY
                annee,
                mois ROWS BETWEEN UNBOUNDED PRECEDING
                AND CURRENT ROW
        ),
        2
    ) AS cumul,
    ROUND(
        montant_total - LAG(montant_total, 1) OVER (
            PARTITION BY
                type_compte
            ORDER BY annee, mois
        ),
        2
    ) AS variation,
    RANK() OVER (
        PARTITION BY
            type_compte
        ORDER BY montant_total DESC
    ) AS rang
FROM depots_mensuels
ORDER BY type_compte, annee, mois;

EXPLAIN FORMAT = TREE
WITH
    activite_mensuelle AS (
        SELECT
            a.id AS compte_id,
            a.num_acc,
            c.name AS client,
            YEAR(t.date) AS annee,
            MONTH(t.date) AS mois,
            COUNT(t.id) AS nb_tx,
            SUM(t.amount) AS volume_mensuel,
            SUM(
                CASE
                    WHEN t.type_id = 3 THEN t.amount
                    ELSE 0
                END
            ) AS retraits_dab
        FROM
            transactions t
            JOIN accounts a ON a.id = t.account_id
            JOIN customers c ON c.id = a.customer_id
        WHERE
            t.status = 'completed'
        GROUP BY
            a.id,
            a.num_acc,
            c.name,
            YEAR(t.date),
            MONTH(t.date)
    ),
    avec_historique AS (
        SELECT
            *,
            ROUND(
                AVG(nb_tx) OVER (
                    PARTITION BY
                        compte_id
                    ORDER BY
                        annee,
                        mois ROWS BETWEEN 3 PRECEDING
                        AND 1 PRECEDING
                ),
                2
            ) AS moy_nb_tx_3m,
            ROUND(
                AVG(volume_mensuel) OVER (
                    PARTITION BY
                        compte_id
                    ORDER BY
                        annee,
                        mois ROWS BETWEEN 3 PRECEDING
                        AND 1 PRECEDING
                ),
                2
            ) AS moy_volume_3m,
            LAG(nb_tx, 1) OVER (
                PARTITION BY
                    compte_id
                ORDER BY annee, mois
            ) AS nb_tx_mois_prec
        FROM activite_mensuelle
    )
SELECT
    CONCAT(
        annee,
        '-',
        LPAD(mois, 2, '0')
    ) AS periode,
    client,
    num_acc,
    nb_tx,
    ROUND(volume_mensuel, 2),
    ROUND(retraits_dab, 2),
    moy_volume_3m
FROM avec_historique
WHERE
    moy_volume_3m IS NOT NULL
LIMIT 10;

SELECT status, COUNT(*) AS nb, ROUND(COUNT(*) * 100 / 150, 1) AS pct
FROM transactions
GROUP BY
    status;

SELECT type_id, COUNT(*) AS nb FROM transactions GROUP BY type_id;

SELECT direction, COUNT(*) AS nb
FROM transactions
GROUP BY
    direction;

SELECT statut, COUNT(*) AS nb FROM accounts GROUP BY statut;

SELECT
    INDEX_NAME,
    COLUMN_NAME,
    SEQ_IN_INDEX,
    CARDINALITY,
    NULLABLE
FROM information_schema.STATISTICS
WHERE
    TABLE_SCHEMA = 'neobank'
    AND TABLE_NAME = 'transactions'
ORDER BY INDEX_NAME, SEQ_IN_INDEX;

EXPLAIN FORMAT = JSON
WITH
    depots AS (
        SELECT YEAR(t.date) AS annee, MONTH(t.date) AS mois, a.type, SUM(t.amount) AS total
        FROM transactions t
            JOIN accounts a ON a.id = t.account_id
        WHERE
            t.type_id = 4
            AND t.direction = 'C'
            AND t.status = 'completed'
        GROUP BY
            YEAR(t.date),
            MONTH(t.date),
            a.type
    )
SELECT *
FROM depots
ORDER BY annee, mois;