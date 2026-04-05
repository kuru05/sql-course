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
            client_id,
            client,
            nb_transactions,
            volume_total,
            total_depots,
            total_debits,
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
    ROUND(volume_total, 2) AS volume_total_eur,
    ROUND(total_depots, 2) AS total_depots_eur,
    ROUND(total_debits, 2) AS total_debits_eur,
    rang_depots,
    rang_activite
FROM classement
WHERE
    rang_volume <= 10
ORDER BY rang_volume;

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
            a.statut AS statut_compte,
            tt.label AS type_transaction,
            t.amount,
            t.direction,
            t.status,
            CASE
                WHEN s.moy_montant IS NOT NULL
                AND t.amount > 3 * s.moy_montant THEN 'Montant anormalement élevé'
            END AS regle_r1,
            CASE
                WHEN a.statut = 'blocked' THEN 'Transaction sur compte bloqué'
            END AS regle_r2,
            CASE
                WHEN fj.nb_jour > 5 THEN CONCAT(
                    'Rafale : ',
                    fj.nb_jour,
                    ' tx ce jour'
                )
            END AS regle_r3,
            CASE
                WHEN t.type_id = 1
                AND t.direction = 'D'
                AND t2.id IS NULL THEN 'Virement sans contrepartie'
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
    statut_compte,
    type_transaction,
    amount,
    direction,
    status,
    CONCAT_WS(
        ' | ',
        regle_r1,
        regle_r2,
        regle_r3,
        regle_r4
    ) AS raisons_alerte
FROM anomalies
WHERE
    regle_r1 IS NOT NULL
    OR regle_r2 IS NOT NULL
    OR regle_r3 IS NOT NULL
    OR regle_r4 IS NOT NULL
ORDER BY date DESC;

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
    ROUND(montant_total, 2) AS montant_total_eur,
    ROUND(montant_moyen, 2) AS montant_moyen_eur,
    nb_comptes_actifs,
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
    ) AS cumul_depots_eur,
    ROUND(
        montant_total - LAG(montant_total, 1) OVER (
            PARTITION BY
                type_compte
            ORDER BY annee, mois
        ),
        2
    ) AS variation_vs_mois_prec,
    RANK() OVER (
        PARTITION BY
            type_compte
        ORDER BY montant_total DESC
    ) AS rang_meilleur_mois
FROM depots_mensuels
ORDER BY type_compte, annee, mois;

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
    COALESCE(
        CAST(annee AS CHAR),
        'TOTAL GÉNÉRAL'
    ) AS annee,
    COALESCE(
        CAST(mois AS CHAR),
        'Sous-total'
    ) AS mois,
    COALESCE(type_compte, 'Tous types') AS type_compte,
    SUM(nb_depots) AS nb_depots,
    ROUND(SUM(montant_total), 2) AS montant_total_eur,
    ROUND(AVG(montant_moyen), 2) AS montant_moyen_eur,
    SUM(nb_comptes_actifs) AS nb_comptes_actifs,
    GROUPING(annee) AS est_total_annee,
    GROUPING(mois) AS est_total_mois,
    GROUPING(type_compte) AS est_total_type
FROM depots_mensuels
GROUP BY
    annee,
    mois,
    type_compte
WITH
    ROLLUP
ORDER BY GROUPING(annee), COALESCE(annee, 9999), GROUPING(mois), COALESCE(mois, 13), GROUPING(type_compte), IFNULL(type_compte, 'ZZZZ');

WITH
    activite_compte AS (
        SELECT
            a.id AS compte_id,
            a.num_acc,
            a.type AS type_compte,
            a.statut,
            a.amount AS solde_actuel,
            c.name AS client,
            COUNT(t.id) AS nb_transactions,
            COALESCE(SUM(t.amount), 0) AS volume_tx,
            COALESCE(AVG(t.amount), 0) AS montant_moyen_tx
        FROM
            accounts a
            JOIN customers c ON c.id = a.customer_id
            LEFT JOIN transactions t ON t.account_id = a.id
            AND t.status = 'completed'
        GROUP BY
            a.id
    ),
    classement_compte AS (
        SELECT
            compte_id,
            num_acc,
            type_compte,
            statut,
            client,
            nb_transactions,
            ROUND(solde_actuel, 2) AS solde_actuel,
            ROUND(volume_tx, 2) AS volume_tx,
            ROUND(montant_moyen_tx, 2) AS montant_moyen_tx,
            DENSE_RANK() OVER (
                ORDER BY nb_transactions DESC
            ) AS rang_activite,
            DENSE_RANK() OVER (
                PARTITION BY
                    type_compte
                ORDER BY nb_transactions DESC
            ) AS rang_dans_type,
            NTILE(4) OVER (
                ORDER BY nb_transactions DESC
            ) AS quartile_activite,
            NTILE(4) OVER (
                ORDER BY solde_actuel DESC
            ) AS quartile_solde,
            ROUND(
                PERCENT_RANK() OVER (
                    ORDER BY nb_transactions DESC
                ) * 100,
                1
            ) AS percentile_activite
        FROM activite_compte
    )
SELECT
    rang_activite,
    rang_dans_type,
    client,
    num_acc,
    type_compte,
    statut,
    nb_transactions,
    solde_actuel,
    volume_tx,
    montant_moyen_tx,
    CONCAT('Q', quartile_activite) AS quartile_activite,
    CONCAT('Q', quartile_solde) AS quartile_solde,
    CONCAT(percentile_activite, '%') AS percentile_activite
FROM classement_compte
WHERE
    nb_transactions > 0
ORDER BY rang_activite
LIMIT 30;

WITH
    activite_compte AS (
        SELECT
            a.id,
            a.type AS type_compte,
            a.statut,
            a.amount AS solde_actuel,
            COUNT(t.id) AS nb_transactions
        FROM
            accounts a
            LEFT JOIN transactions t ON t.account_id = a.id
            AND t.status = 'completed'
        GROUP BY
            a.id,
            a.type,
            a.statut,
            a.amount
    )
SELECT
    type_compte,
    statut,
    COUNT(*) AS nb_comptes,
    SUM(nb_transactions) AS total_transactions,
    ROUND(AVG(solde_actuel), 2) AS solde_moyen,
    ROUND(SUM(solde_actuel), 2) AS solde_total,
    0 AS est_sous_total_type,
    0 AS est_sous_total_statut
FROM activite_compte
GROUP BY
    type_compte,
    statut
UNION ALL
SELECT type_compte, '★ SOUS-TOTAL', COUNT(*), SUM(nb_transactions), ROUND(AVG(solde_actuel), 2), ROUND(SUM(solde_actuel), 2), 1, 0
FROM activite_compte
GROUP BY
    type_compte
UNION ALL
SELECT '★ SOUS-TOTAL', statut, COUNT(*), SUM(nb_transactions), ROUND(AVG(solde_actuel), 2), ROUND(SUM(solde_actuel), 2), 0, 1
FROM activite_compte
GROUP BY
    statut
UNION ALL
SELECT '★ GRAND TOTAL', '★ TOUS', COUNT(*), SUM(nb_transactions), ROUND(AVG(solde_actuel), 2), ROUND(SUM(solde_actuel), 2), 1, 1
FROM activite_compte
ORDER BY
    est_sous_total_type,
    type_compte,
    est_sous_total_statut,
    statut;

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
            compte_id,
            num_acc,
            client,
            annee,
            mois,
            nb_tx,
            volume_mensuel,
            retraits_dab,
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
            ROUND(
                AVG(retraits_dab) OVER (
                    PARTITION BY
                        compte_id
                    ORDER BY
                        annee,
                        mois ROWS BETWEEN 3 PRECEDING
                        AND 1 PRECEDING
                ),
                2
            ) AS moy_retraits_3m,
            LAG(nb_tx, 1) OVER (
                PARTITION BY
                    compte_id
                ORDER BY annee, mois
            ) AS nb_tx_mois_prec
        FROM activite_mensuelle
    ),
    anomalies_comportement AS (
        SELECT
            compte_id,
            num_acc,
            client,
            annee,
            mois,
            nb_tx,
            ROUND(volume_mensuel, 2) AS volume_mensuel,
            ROUND(retraits_dab, 2) AS retraits_dab,
            moy_nb_tx_3m,
            moy_volume_3m,
            moy_retraits_3m,
            CASE
                WHEN moy_volume_3m > 0 THEN ROUND(
                    volume_mensuel / moy_volume_3m,
                    2
                )
            END AS ratio_volume,
            CASE
                WHEN moy_volume_3m > 0
                AND volume_mensuel > 2 * moy_volume_3m THEN CONCAT(
                    'Volume 2x moy. 3 mois (',
                    moy_volume_3m,
                    '€)'
                )
            END AS signal_volume,
            CASE
                WHEN moy_retraits_3m > 0
                AND retraits_dab > 3 * moy_retraits_3m THEN CONCAT(
                    'Retraits DAB 3x moy. (',
                    moy_retraits_3m,
                    '€)'
                )
            END AS signal_retraits,
            CASE
                WHEN nb_tx_mois_prec = 0
                AND nb_tx > 5 THEN CONCAT(
                    'Inactif M-1, ',
                    nb_tx,
                    ' tx ce mois'
                )
            END AS signal_inactivite,
            CASE
                WHEN moy_nb_tx_3m > 0
                AND nb_tx > 3 * moy_nb_tx_3m THEN CONCAT(
                    'Freq. 3x moy. (',
                    moy_nb_tx_3m,
                    ' tx/mois)'
                )
            END AS signal_frequence
        FROM avec_historique
        WHERE
            moy_volume_3m IS NOT NULL
            OR nb_tx_mois_prec IS NOT NULL
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
    volume_mensuel,
    retraits_dab,
    moy_volume_3m AS moy_volume_3mois,
    COALESCE(ratio_volume, '—') AS ratio_vs_historique,
    CONCAT_WS(
        ' | ',
        signal_volume,
        signal_retraits,
        signal_inactivite,
        signal_frequence
    ) AS signaux_anomalie
FROM anomalies_comportement
WHERE
    signal_volume IS NOT NULL
    OR signal_retraits IS NOT NULL
    OR signal_inactivite IS NOT NULL
    OR signal_frequence IS NOT NULL
ORDER BY annee DESC, mois DESC, ratio_volume DESC;