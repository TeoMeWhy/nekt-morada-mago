WITH tb_cross_join AS (

    SELECT t1.dtRef,
           t1.IdCliente,
           t1.descLifeCycle,
           t2.descLifeCycle,
           CASE WHEN t2.descLifeCycle = '02-FIEL' THEN 1 ELSE 0 END AS flFiel,
           ROW_NUMBER() OVER (PARTITION BY t1.IdCliente ORDER BY random(42)) as RandomCol

    FROM life_cycle AS t1

    LEFT JOIN life_cycle AS t2
    ON t1.IdCliente = t2.IdCliente
    AND (t1.dtRef + INTERVAL 28 day) = date(t2.dtRef)

    WHERE ((t1.dtRef >= '2024-03-01' AND t1.dtRef <= '2025-08-01')
            OR t1.dtRef + interval 1 day ='2025-09-01')
    AND DAY(t1.dtRef + interval 1 day) = 1
    AND t1.descLifeCycle <> '05-ZUMBI'

),

tb_cohort AS (

    SELECT t1.dtRef AS dtref,
        t1.IdCliente AS idcliente,
        t1.flFiel AS flfiel

    FROM tb_cross_join AS t1
    WHERE RandomCol <= 2
    ORDER BY IdCliente, dtRef

),

tb_join AS (

    SELECT t1.*,
        t2.idadedias, 
        t2.qtdeAtivacaoVida AS qtdeativacaovida,
        t2.qtdeAtivacaoD7 AS qtdeativacaod7,
        t2.qtdeAtivacaoD14 AS qtdeativacaod14,
        t2.qtdeAtivacaoD28 AS qtdeativacaod28,
        t2.qtdeAtivacaoD56 AS qtdeativacaod56,
        t2.qtdeTransacaoVida AS qtdetransacaovida,
        t2.qtdeTransacaoD7 AS qtdetransacaod7,
        t2.qtdeTransacaoD14 AS qtdetransacaod14,
        t2.qtdeTransacaoD28 AS qtdetransacaod28,
        t2.qtdeTransacaoD56 AS qtdetransacaod56,
        t2.saldoVida AS saldovida,
        t2.saldoD7 AS saldod7,
        t2.saldoD14 AS saldod14,
        t2.saldoD28 AS saldod28,
        t2.saldoD56 AS saldod56,
        t2.qtdePontosPosVida AS qtdepontosposvida,
        t2.qtdePontosPosD7 AS qtdepontosposd7,
        t2.qtdePontosPosD14 AS qtdepontosposd14,
        t2.qtdePontosPosD28 AS qtdepontosposd28,
        t2.qtdePontosPosD56 AS qtdepontosposd56,
        t2.qtdePontosNegVida AS qtdepontosnegvida,
        t2.qtdePontosNegD7 AS qtdepontosnegd7,
        t2.qtdePontosNegD14 AS qtdepontosnegd14,
        t2.qtdePontosNegD28 AS qtdepontosnegd28,
        t2.qtdePontosNegD56 AS qtdepontosnegd56,
        t2.qtdeTransacaoManha AS qtdetransacaomanha,
        t2.qtdeTransacaoTarde AS qtdetransacaotarde,
        t2.qtdeTransacaoNoite AS qtdetransacaonoite,
        t2.pctTransacaoManha AS pcttransacaomanha,
        t2.pctTransacaoTarde AS pcttransacaotarde,
        t2.pctTransacaoNoite AS pcttransacaonoite,
        t2.QtdeTransacaoDiaVida AS qtdetransacaodiavida,
        t2.QtdeTransacaoDiaD7 AS qtdetransacaodiad7,
        t2.QtdeTransacaoDiaD14 AS qtdetransacaodiad14,
        t2.QtdeTransacaoDiaD28 AS qtdetransacaodiad28,
        t2.QtdeTransacaoDiaD56 AS qtdetransacaodiad56,
        t2.pctAtivacaoMAU AS pctativacaomau,
        t2.qtdeHorasVida AS qtdehorasvida,
        t2.qtdeHorasD7 AS qtdehorasd7,
        t2.qtdeHorasD14 AS qtdehorasd14,
        t2.qtdeHorasD28 AS qtdehorasd28,
        t2.qtdeHorasD56 AS qtdehorasd56,
        t2.avgIntervaloDiasVida AS avgintervalodiasvida,
        t2.avgIntervaloDiasD28 AS avgintervalodiasd28,
        t2.qteChatMessage AS qtechatmessage,
        t2.qteAirflowLover AS qteairflowlover,
        t2.qteRLover AS qterlover,
        t2.qteResgatarPonei AS qteresgatarponei,
        t2.qteListadepresenca AS qtelistadepresenca,
        t2.qtePresencaStreak AS qtepresencastreak,
        t2.qteTrocaStreamElements AS qtetrocastreamelements,
        t2.qteReembolsoStreamElements AS qtereembolsostreamelements,
        t2.qtdeRPG AS qtderpg,
        t2.qtdeChurnModel AS qtdechurnmodel,
        t3.qtdeFrequencia AS qtdefrequencia,
        t3.descLifeCycleAtual AS desclifecycleatual,
        t3.descLifeCycleD28 AS desclifecycled28,
        t3.pctCurioso AS pctcurioso,
        t3.pctFiel AS pctfiel,
        t3.pctTurista AS pctturista,
        t3.pctDesencantada AS pctdesencantada,
        t3.pctZumbi AS pctzumbi,
        t3.pctReconquistado AS pctreconquistado,
        t3.pctReborn AS pctreborn,
        t3.avgFreqGrupo AS avgfreqgrupo,
        t3.ratioFreqGrupo AS ratiofreqgrupo,
        t4.qtdeCursosCompletos AS qtdecursoscompletos,
        t4.qtdeCursosIncompletos AS qtdecursosincompletos,
        t4.carreira AS carreira,
        t4.coletaDados2024 AS coletadados2024,
        t4.dsDatabricks2024 AS dsdatabricks2024,
        t4.dsPontos2024 AS dspontos2024,
        t4.estatistica2024 AS estatistica2024,
        t4.estatistica2025 AS estatistica2025,
        t4.github2024 AS github2024,
        t4.github2025 AS github2025,
        t4.iaCanal2025 AS iacanal2025,
        t4.lagoMago2024 AS lagomago2024,
        t4.machineLearning2025 AS machinelearning2025,
        t4.matchmakingTramparDeCasa2024 AS matchmakingtrampardecasa2024,
        t4.ml2024 AS ml2024,
        t4.mlflow2025 AS mlflow2025,
        t4.pandas2024 AS pandas2024,
        t4.pandas2025 AS pandas2025,
        t4.python2024 AS python2024,
        t4.python2025 AS python2025,
        t4.sql2020 AS sql2020,
        t4.sql2025 AS sql2025,
        t4.streamlit2025 AS streamlit2025,
        t4.tramparLakehouse2024 AS tramparlakehouse2024,
        t4.tseAnalytics2024 AS tseanalytics2024,
        t4.qtdDiasUltiAtividade AS qtddiasultiatividade

    FROM tb_cohort AS t1

    LEFT JOIN fs_transacional AS t2
    ON t1.IdCliente = t2.IdCliente
    AND t1.dtRef = t2.dtRef

    LEFT JOIN fs_life_cycle AS t3
    ON t1.IdCliente = t3.IdCliente
    AND t1.dtRef = t3.dtRef

    LEFT JOIN fs_education AS t4
    ON t1.IdCliente = t4.IdCliente
    AND t1.dtRef = t4.dtRef

    WHERE t3.dtRef IS NOT NULL

)

SELECT *
FROM tb_join