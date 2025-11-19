WITH tb_transacao AS (

    SELECT *,
           date(DtCriacao) AS dtDia,
           HOUR(DtCriacao) AS dtHora
    
    FROM points_transacoes
    WHERE dtCriacao < '{date}'

),

tb_agg_transacao AS (

    SELECT IdCliente,

            max(DATE_DIFF(day, dtCriacao, '{date}')) AS idadeDias,

            count(DISTINCT dtDia) AS qtdeAtivacaoVida,
            count(DISTINCT CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 7 THEN dtDia END) AS qtdeAtivacaoD7,
            count(DISTINCT CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 14 THEN dtDia END) AS qtdeAtivacaoD14,
            count(DISTINCT CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 28 THEN dtDia END) AS qtdeAtivacaoD28,
            count(DISTINCT CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 56 THEN dtDia END) AS qtdeAtivacaoD56,

            count(DISTINCT IdTransacao) AS qtdeTransacaoVida,
            count(DISTINCT CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 7 THEN IdTransacao END) AS qtdeTransacaoD7,
            count(DISTINCT CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 14 THEN IdTransacao END) AS qtdeTransacaoD14,
            count(DISTINCT CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 28 THEN IdTransacao END) AS qtdeTransacaoD28,
            count(DISTINCT CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 56 THEN IdTransacao END) AS qtdeTransacaoD56,

            sum(qtdePontos) AS saldoVida,
            sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 7 THEN qtdePontos ELSE 0 END) AS saldoD7,
            sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 14 THEN qtdePontos ELSE 0 END) AS saldoD14,
            sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 28 THEN qtdePontos ELSE 0 END) AS saldoD28,
            sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 56 THEN qtdePontos ELSE 0 END) AS saldoD56,

            sum(CASE WHEN qtdePontos > 0 THEN qtdePontos ELSE 0 END ) AS qtdePontosPosVida,
            sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 7 AND qtdePontos > 0 THEN qtdePontos ELSE 0 END) AS qtdePontosPosD7,
            sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 14 AND qtdePontos > 0 THEN qtdePontos ELSE 0 END) AS qtdePontosPosD14,
            sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 28 AND qtdePontos > 0 THEN qtdePontos ELSE 0 END) AS qtdePontosPosD28,
            sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 56 AND qtdePontos > 0 THEN qtdePontos ELSE 0 END) AS qtdePontosPosD56,

            sum(CASE WHEN qtdePontos < 0 THEN qtdePontos ELSE 0 END ) AS qtdePontosNegVida,
            sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 7 AND qtdePontos < 0 THEN qtdePontos ELSE 0 END) AS qtdePontosNegD7,
            sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 14 AND qtdePontos < 0 THEN qtdePontos ELSE 0 END) AS qtdePontosNegD14,
            sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 28 AND qtdePontos < 0 THEN qtdePontos ELSE 0 END) AS qtdePontosNegD28,
            sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 56 AND qtdePontos < 0 THEN qtdePontos ELSE 0 END) AS qtdePontosNegD56,

            count(CASE WHEN dtHora BETWEEN 10 AND 14 THEN IdTransacao END) AS qtdeTransacaoManha,
            count(CASE WHEN dtHora BETWEEN 15 AND 21 THEN IdTransacao END) AS qtdeTransacaoTarde,
            count(CASE WHEN dtHora > 21 OR dtHora < 10 THEN IdTransacao END) AS qtdeTransacaoNoite,

            1. * count(CASE WHEN dtHora BETWEEN 10 AND 14 THEN IdTransacao END) / count(IdTransacao) AS pctTransacaoManha,
            1. * count(CASE WHEN dtHora BETWEEN 15 AND 21 THEN IdTransacao END) / count(IdTransacao) AS pctTransacaoTarde,
            1. * count(CASE WHEN dtHora > 21 OR dtHora < 10 THEN IdTransacao END) / count(IdTransacao) AS pctTransacaoNoite

    FROM tb_transacao
    GROUP BY IdCliente

),
          
tb_agg_calc AS (

    SELECT 
            *,
            COALESCE(1. * qtdeTransacaoVida / qtdeAtivacaoVida,0) AS QtdeTransacaoDiaVida,
            COALESCE(1. * qtdeTransacaoD7 / qtdeAtivacaoD7,0) AS QtdeTransacaoDiaD7,
            COALESCE(1. * qtdeTransacaoD14 / qtdeAtivacaoD14,0) AS QtdeTransacaoDiaD14,
            COALESCE(1. * qtdeTransacaoD28 / qtdeAtivacaoD28,0) AS QtdeTransacaoDiaD28,
            COALESCE(1. * qtdeTransacaoD56 / qtdeAtivacaoD56,0) AS QtdeTransacaoDiaD56,
            COALESCE(1. * qtdeAtivacaoD28 / 28, 0) AS pctAtivacaoMAU

    FROM tb_agg_transacao

),

tb_horas_dia AS (

    SELECT IdCliente,
           dtDia,
           (max(unix_timestamp(DtCriacao)) - min(unix_timestamp(DtCriacao)))/(60*60) AS duracao

    FROM tb_transacao
    GROUP BY IdCliente, dtDia

),

tb_hora_cliente AS (

    SELECT IdCliente,
           sum(duracao) AS qtdeHorasVida,
           sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 7 THEN duracao ELSE 0 END) AS qtdeHorasD7,
           sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 14 THEN duracao ELSE 0 END) AS qtdeHorasD14,
           sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 28 THEN duracao ELSE 0 END) AS qtdeHorasD28,
           sum(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 56 THEN duracao ELSE 0 END) AS qtdeHorasD56

    FROM tb_horas_dia
    GROUP BY IdCliente
),

tb_lag_dia AS (

    SELECT idCliente,
           dtDia,
           LAG(dtDia) OVER (PARTITION BY idCliente order by dtDia) AS lagDia

    FROM tb_horas_dia

),

tb_intervalo_dias AS (

    SELECT IdCliente,
           avg(DATE_DIFF(day, lagDia, dtDia)) AS avgIntervaloDiasVida,
           avg(CASE WHEN DATE_DIFF(day, dtDia, date('{date}')) <= 28 THEN DATE_DIFF(day, lagDia, dtDia) END) AS avgIntervaloDiasD28

    FROM tb_lag_dia
    GROUP BY idCliente

),
          
tb_share_produtos AS (

    SELECT 
        idCliente,
        1. * COUNT(CASE WHEN descNomeProduto = 'ChatMessage' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteChatMessage,
        1. * COUNT(CASE WHEN descNomeProduto = 'Airflow Lover' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteAirflowLover,
        1. * COUNT(CASE WHEN descNomeProduto = 'R Lover' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteRLover,
        1. * COUNT(CASE WHEN descNomeProduto = 'Resgatar Ponei' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteResgatarPonei,
        1. * COUNT(CASE WHEN descNomeProduto = 'Lista de presença' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteListadepresenca,
        1. * COUNT(CASE WHEN descNomeProduto = 'Presença Streak' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qtePresencaStreak,
        1. * COUNT(CASE WHEN descNomeProduto = 'Troca de Pontos StreamElements' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteTrocaStreamElements,
        1. * COUNT(CASE WHEN descNomeProduto = 'Reembolso: Troca de Pontos StreamElements' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qteReembolsoStreamElements,
        1. * COUNT(CASE WHEN descCategoriaProduto = 'rpg' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qtdeRPG,
        1. * COUNT(CASE WHEN descCategoriaProduto = 'churn_model' THEN t1.IdTransacao END) / count(t1.IdTransacao) AS qtdeChurnModel

    FROM tb_transacao AS t1

    LEFT JOIN points_transacao_produto AS t2
    ON t1.IdTransacao = t2.IdTransacao

    LEFT JOIN points_produtos AS t3
    ON t2.IdProduto = t3.IdProduto

    GROUP BY idCliente

),

tb_join AS (

    SELECT t1.*,
        t2.qtdeHorasVida,
        t2.qtdeHorasD7,
        t2.qtdeHorasD14,
        t2.qtdeHorasD28,
        t2.qtdeHorasD56,
        t3.avgIntervaloDiasVida,
        t3.avgIntervaloDiasD28,
        t4.qteChatMessage,
        t4.qteAirflowLover,
        t4.qteRLover,
        t4.qteResgatarPonei,
        t4.qteListadepresenca,
        t4.qtePresencaStreak,
        t4.qteTrocaStreamElements,
        t4.qteReembolsoStreamElements,
        t4.qtdeRPG,
        t4.qtdeChurnModel

    FROM tb_agg_calc AS t1

    LEFT JOIN tb_hora_cliente AS t2
    ON t1.IdCliente = t2.IdCliente

    LEFT JOIN tb_intervalo_dias AS t3
    ON t1.IdCliente = t3.IdCliente

    LEFT JOIN tb_share_produtos AS t4
    ON t1.idCliente = t4.idCliente

)
          
SELECT date('{date}') - INTERVAL 1 DAYS AS dtRef,
       *

FROM tb_join