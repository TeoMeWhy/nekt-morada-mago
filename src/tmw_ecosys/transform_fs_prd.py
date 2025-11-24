import nekt
import datetime


def date_range(start, stop, monthly=False):
    dates = []
    while start <= stop:
        dates.append(start)
        dt_start = datetime.datetime.strptime(start, '%Y-%m-%d') + datetime.timedelta(days=1)
        start = datetime.datetime.strftime(dt_start, '%Y-%m-%d')
        
    if monthly:
        return [i for i in dates if i.endswith("01")]
    return dates


def exec_iterable_query(layer, folder, table_name, query, dates_origin):

    dates = dates_origin.copy()

    try:
        df_all = nekt.load_table(layer_name=layer, table_name=table_name)
        df_all = df_all.union(spark.sql(query.format(date=dates.pop(0))))
        
    except Exception:
        df_all = spark.sql(query.format(date=dates.pop(0)))

    for d in dates:
        df_all = df_all.union(spark.sql(query.format(date=d)))

    df_all = df_all.dropDuplicates(['dtRef', 'idCliente'])

    nekt.save_table(
        df=df_all,
        layer_name=layer,
        table_name=table_name,
        folder_name=folder,
    )


def exec_long_dates(layer, folder, table_name, query, dates_origin, window_size=30):

    dates = dates_origin.copy()

    while len(dates)> window_size:
        dates_tmp = dates[:window_size]
        dates = dates[window_size:]
        exec_iterable_query(layer, folder, table_name, query, dates_tmp)

    exec_iterable_query(layer, folder, table_name, query, dates)



(nekt.load_table(layer_name="Bronze", table_name="points_transacoes")
     .createOrReplaceTempView("points_transacoes"))

(nekt.load_table(layer_name="Bronze", table_name="points_transacao_produto")
     .createOrReplaceTempView("points_transacao_produto"))

(nekt.load_table(layer_name="Bronze", table_name="points_produtos")
     .createOrReplaceTempView("points_produtos"))

(nekt.load_table(layer_name="Bronze", table_name="education_cursos_episodios_completos")
     .createOrReplaceTempView("education_cursos_episodios_completos"))

(nekt.load_table(layer_name="Bronze", table_name="education_cursos_episodios")
     .createOrReplaceTempView("education_cursos_episodios"))

(nekt.load_table(layer_name="Bronze", table_name="education_recompensas_usuarios")
     .createOrReplaceTempView("education_recompensas_usuarios"))

(nekt.load_table(layer_name="Bronze", table_name="education_habilidades_usuarios")
     .createOrReplaceTempView("education_habilidades_usuarios"))

(nekt.load_table(layer_name="Bronze", table_name="education_usuarios_tmw")
     .createOrReplaceTempView("education_usuarios_tmw"))

(nekt.load_table(layer_name="Silver", table_name="life_cycle")
     .createOrReplaceTempView("tmw_ecosys_life_cycle"))



# RANGE DE DATAS
start = datetime.datetime.now().strftime("%Y-%m-%d")
# start = "2025-01-01"
stop = datetime.datetime.now().strftime("%Y-%m-%d")
dates = date_range(start, stop, monthly=False)

# SPARK SESSION
spark = nekt.get_spark_session()

query_fs_transacional = """WITH
tb_transacao AS (

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
"""

query_fs_education = """

WITH tb_usuario_cursos AS (
    SELECT idUsuario,
           descSlugCurso,
           COUNT(descSlugCursoEpisodio) AS qtdeEps

    FROM education_cursos_episodios_completos
    WHERE DtCriacao < DATE('{date}')
    GROUP BY idUsuario, descSlugCurso
),

tb_cursos_total_eps AS (

    SELECT descSlugCurso,
           COUNT(descEpisodio) as qtdeTotalEps
    FROM education_cursos_episodios
    GROUP BY descSlugCurso

),

tb_pct_cursos AS (

    SELECT t1.idUsuario,
            t1.descSlugCurso,
            1. * t1.qtdeEps / t2.qtdeTotalEps AS pctCursoCompleto

    FROM tb_usuario_cursos AS t1

    LEFT JOIN tb_cursos_total_eps AS t2
    ON t1.descSlugCurso = t2.descSlugCurso

),

tb_pct_cursos_pivot AS (

    SELECT idUsuario,

        SUM(CASE WHEN pctCursoCompleto = 1 then 1 else 0 end) AS qtdeCursosCompletos,
        SUM(CASE WHEN pctCursoCompleto > 0 and pctCursoCompleto < 1 then 1 else 0 end) AS qtdeCursosIncompletos,
        SUM(CASE WHEN descSlugCurso = 'carreira' THEN pctCursoCompleto ELSE 0 END) AS carreira,
        SUM(CASE WHEN descSlugCurso = 'coleta-dados-2024' THEN pctCursoCompleto ELSE 0 END) AS coletaDados2024,
        SUM(CASE WHEN descSlugCurso = 'ds-databricks-2024' THEN pctCursoCompleto ELSE 0 END) AS dsDatabricks2024,
        SUM(CASE WHEN descSlugCurso = 'ds-pontos-2024' THEN pctCursoCompleto ELSE 0 END) AS dsPontos2024,
        SUM(CASE WHEN descSlugCurso = 'estatistica-2024' THEN pctCursoCompleto ELSE 0 END) AS estatistica2024,
        SUM(CASE WHEN descSlugCurso = 'estatistica-2025' THEN pctCursoCompleto ELSE 0 END) AS estatistica2025,
        SUM(CASE WHEN descSlugCurso = 'github-2024' THEN pctCursoCompleto ELSE 0 END) AS github2024,
        SUM(CASE WHEN descSlugCurso = 'github-2025' THEN pctCursoCompleto ELSE 0 END) AS github2025,
        SUM(CASE WHEN descSlugCurso = 'ia-canal-2025' THEN pctCursoCompleto ELSE 0 END) AS iaCanal2025,
        SUM(CASE WHEN descSlugCurso = 'lago-mago-2024' THEN pctCursoCompleto ELSE 0 END) AS lagoMago2024,
        SUM(CASE WHEN descSlugCurso = 'machine-learning-2025' THEN pctCursoCompleto ELSE 0 END) AS machineLearning2025,
        SUM(CASE WHEN descSlugCurso = 'matchmaking-trampar-de-casa-2024' THEN pctCursoCompleto ELSE 0 END) AS matchmakingTramparDeCasa2024,
        SUM(CASE WHEN descSlugCurso = 'ml-2024' THEN pctCursoCompleto ELSE 0 END) AS ml2024,
        SUM(CASE WHEN descSlugCurso = 'mlflow-2025' THEN pctCursoCompleto ELSE 0 END) AS mlflow2025,
        SUM(CASE WHEN descSlugCurso = 'pandas-2024' THEN pctCursoCompleto ELSE 0 END) AS pandas2024,
        SUM(CASE WHEN descSlugCurso = 'pandas-2025' THEN pctCursoCompleto ELSE 0 END) AS pandas2025,
        SUM(CASE WHEN descSlugCurso = 'python-2024' THEN pctCursoCompleto ELSE 0 END) AS python2024,
        SUM(CASE WHEN descSlugCurso = 'python-2025' THEN pctCursoCompleto ELSE 0 END) AS python2025,
        SUM(CASE WHEN descSlugCurso = 'sql-2020' THEN pctCursoCompleto ELSE 0 END) AS sql2020,
        SUM(CASE WHEN descSlugCurso = 'sql-2025' THEN pctCursoCompleto ELSE 0 END) AS sql2025,
        SUM(CASE WHEN descSlugCurso = 'streamlit-2025' THEN pctCursoCompleto ELSE 0 END) AS streamlit2025,
        SUM(CASE WHEN descSlugCurso = 'trampar-lakehouse-2024' THEN pctCursoCompleto ELSE 0 END) AS tramparLakehouse2024,
        SUM(CASE WHEN descSlugCurso = 'tse-analytics-2024' THEN pctCursoCompleto ELSE 0 END) AS tseAnalytics2024

    FROM tb_pct_cursos

    GROUP BY idUsuario  

),

tb_atividade AS (

    SELECT 
        idUsuario,
        max(dtRecompensa) as dtCriacao

    FROM education_recompensas_usuarios
    WHERE dtRecompensa < DATE('{date}')
    GROUP BY idUsuario

    UNION ALL

    SELECT 
        idUsuario,
        max(dtCriacao) AS dtCriacao

    FROM education_habilidades_usuarios
    WHERE DtCriacao < DATE('{date}')
    GROUP BY idUsuario

    UNION ALL

    SELECT
        idUsuario,
        max(dtCriacao) AS dtCriacao

    FROM education_cursos_episodios_completos
    WHERE DtCriacao < DATE('{date}')
    GROUP BY idUsuario

),

tb_ultima_atividade AS (

    SELECT idUsuario,
            MIN( DATE_DIFF(day, DATE('{date}'), dtCriacao)) AS qtdDiasUltiAtividade
    FROM tb_atividade
    GROUP BY idUsuario

),

tb_join AS (

    SELECT t3.idTMWCliente AS idCliente,
        t1.qtdeCursosCompletos,
        t1.qtdeCursosIncompletos,
        t1.carreira,
        t1.coletaDados2024,
        t1.dsDatabricks2024,
        t1.dsPontos2024,
        t1.estatistica2024,
        t1.estatistica2025,
        t1.github2024,
        t1.github2025,
        t1.iaCanal2025,
        t1.lagoMago2024,
        t1.machineLearning2025,
        t1.matchmakingTramparDeCasa2024,
        t1.ml2024,
        t1.mlflow2025,
        t1.pandas2024,
        t1.pandas2025,
        t1.python2024,
        t1.python2025,
        t1.sql2020,
        t1.sql2025,
        t1.streamlit2025,
        t1.tramparLakehouse2024,
        t1.tseAnalytics2024,
        t2.qtdDiasUltiAtividade

    FROM tb_pct_cursos_pivot as t1

    LEFT JOIN tb_ultima_atividade AS t2
    ON t1.idUsuario = t2.idUsuario

    INNER JOIN education_usuarios_tmw AS t3
    ON t1.idUsuario = t3.idUsuario

)

SELECT DATE('{date}') - INTERVAL 1 DAYS AS dtRef,
       *

FROM tb_join

"""

query_fs_life_cycle = """

WITH tb_life_cycle_atual AS (

    SELECT IdCliente,
           qtdeFrequencia,
           descLifeCycle AS descLifeCycleAtual

    FROM tmw_ecosys_life_cycle
    WHERE dtRef = date(date_add(day, -1, DATE('{date}')))
),

tb_life_cycle_D28 AS (

    SELECT IdCliente,
        descLifeCycle AS descLifeCycleD28

    FROM tmw_ecosys_life_cycle
    WHERE dtRef = date(date_add(day, -29, DATE('{date}')))
),

tb_share_ciclos AS (

    SELECT idCliente,
            1. * SUM(CASE WHEN descLifeCycle = '01-CURIOSO' THEN 1 ELSE 0 END) / COUNT(*) AS pctCurioso,
            1. * SUM(CASE WHEN descLifeCycle = '02-FIEL' THEN 1 ELSE 0 END) / COUNT(*) AS pctFiel,
            1. * SUM(CASE WHEN descLifeCycle = '03-TURISTA' THEN 1 ELSE 0 END) / COUNT(*) AS pctTurista,
            1. * SUM(CASE WHEN descLifeCycle = '04-DESENCANTADA' THEN 1 ELSE 0 END) / COUNT(*) AS pctDesencantada,
            1. * SUM(CASE WHEN descLifeCycle = '05-ZUMBI' THEN 1 ELSE 0 END) / COUNT(*) AS pctZumbi,
            1. * SUM(CASE WHEN descLifeCycle = '02-RECONQUISTADO' THEN 1 ELSE 0 END) / COUNT(*) AS pctReconquistado,
            1. * SUM(CASE WHEN descLifeCycle = '02-REBORN' THEN 1 ELSE 0 END) / COUNT(*) AS pctReborn

    FROM tmw_ecosys_life_cycle
    WHERE dtRef < DATE('{date}')
    GROUP BY idCliente

),


tb_avg_ciclo AS (

    SELECT descLifeCycleAtual,
          AVG(qtdeFrequencia) AS avgFreqGrupo

    FROM tb_life_cycle_atual
    GROUP BY descLifeCycleAtual

),

tb_join AS (

    SELECT t1.*,
        t2.descLifeCycleD28,
        t3.pctCurioso,
        t3.pctFiel,
        t3.pctTurista,
        t3.pctDesencantada,
        t3.pctZumbi,
        t3.pctReconquistado,
        t3.pctReborn,
        t4.avgFreqGrupo,
        1.* t1.qtdeFrequencia / t4.avgFreqGrupo AS ratioFreqGrupo

    FROM tb_life_cycle_atual AS t1
    LEFT JOIN tb_life_cycle_D28 AS t2
    ON t1.IdCliente = t2.IdCliente

    LEFT JOIN tb_share_ciclos AS t3
    ON t1.idCliente = t3.idCliente

    LEFT JOIN tb_avg_ciclo AS t4
    ON t1.descLifeCycleAtual = t4.descLifeCycleAtual

)


SELECT date(date_add(day, -1, DATE('{date}'))) AS dtRef,
       *

FROM tb_join

"""


df_fs_transacional = exec_long_dates(layer="Silver",
                                    folder="tmw_ecosys",
                                    table_name="fs_transacional",
                                    query=query_fs_transacional,
                                    dates_origin=dates.copy(),
                                    window_size=12)


df_fs_education = exec_long_dates(layer="Silver",
                                    folder="tmw_ecosys",
                                    table_name="fs_education",
                                    query=query_fs_education,
                                    dates_origin=dates.copy(),
                                    window_size=12)


df_fs_life_cycle = exec_long_dates(layer="Silver",
                                    folder="tmw_ecosys",
                                    table_name="fs_life_cycle",
                                    query=query_fs_life_cycle,
                                    dates_origin=dates.copy(),
                                    window_size=12)