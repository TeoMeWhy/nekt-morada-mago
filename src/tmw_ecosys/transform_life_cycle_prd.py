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


def exec_iterable_query(layer, folder, table_name, query, dates):

    try:
        df_all = nekt.load_table(layer_name=layer, table_name=table_name)
        df_all.union(spark.sql(query.format(date=dates.pop(0))))
        
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

    return df_all


def exec_long_dates(layer, folder, table_name, query, dates):

    while len(dates)> 30:
        dates_tmp = dates[:30]
        dates = dates[30:]
        exec_iterable_query(layer, folder, table_name, query, dates_tmp)

    exec_iterable_query(layer, folder, table_name, query, dates)



(nekt.load_table(layer_name="Bronze", table_name="points_transacoes")
     .createOrReplaceTempView("points_transacoes"))

(nekt.load_table(layer_name="Bronze", table_name="points_transacao_produto")
     .createOrReplaceTempView("points_transacao_produto"))

(nekt.load_table(layer_name="Bronze", table_name="points_produtos")
     .createOrReplaceTempView("points_produtos"))


# RANGE DE DATAS
start = datetime.datetime.now().strftime("%Y-%m-%d")
start = "2024-04-01"
stop = datetime.datetime.now().strftime("%Y-%m-%d")
stop = "2024-06-01"
dates = date_range(start, stop, monthly=False)

# SPARK SESSION
spark = nekt.get_spark_session()

query_life_cycle = """WITH
	tb_daily AS (
		SELECT DISTINCT
			IdCliente,
			DATE(DtCriacao) AS dtDia
		FROM
			points_transacoes
		WHERE
			DtCriacao < DATE('{date}')
	),
	tb_idade AS (
		SELECT
			IdCliente,
			MAX(DATE_DIFF(day, dtDia, DATE('{date}'))) AS qtdeDiasPrimTransacao,
			MIN(DATE_DIFF(day, dtDia, DATE('{date}'))) AS qtdeDiasUltTransacao
		FROM
			tb_daily
		GROUP BY
			IdCliente
	),
	tb_rn AS (
		SELECT
			*,
			row_number() OVER (
				PARTITION BY
					IdCliente
				ORDER BY
					dtDia DESC
			) AS rnDia
		FROM
			tb_daily
	),
	tb_penultima_ativacao AS (
		SELECT
			*,
			DATE_DIFF(day, dtDia, DATE('{date}')) AS qtdeDiasPenultimaTransacao
		FROM
			tb_rn
		WHERE
			rnDia = 2
	),
	tb_life_cycle AS (
		SELECT
			t1.*,
			t2.qtdeDiasPenultimaTransacao,
			CASE
				WHEN qtdeDiasPrimTransacao <= 7 THEN '01-CURIOSO'
				WHEN qtdeDiasUltTransacao <= 7
				AND qtdeDiasPenultimaTransacao - qtdeDiasUltTransacao <= 14 THEN '02-FIEL'
				WHEN qtdeDiasUltTransacao BETWEEN 8 AND 14  THEN '03-TURISTA'
				WHEN qtdeDiasUltTransacao BETWEEN 15 AND 28  THEN '04-DESENCANTADA'
				WHEN qtdeDiasUltTransacao > 28 THEN '05-ZUMBI'
				WHEN qtdeDiasUltTransacao <= 7
				AND qtdeDiasPenultimaTransacao - qtdeDiasUltTransacao BETWEEN 15 AND 27  THEN '02-RECONQUISTADO'
				WHEN qtdeDiasUltTransacao <= 7
				AND qtdeDiasPenultimaTransacao - qtdeDiasUltTransacao > 27 THEN '02-REBORN'
			END AS descLifeCycle
		FROM
			tb_idade AS t1
			LEFT JOIN tb_penultima_ativacao AS t2 ON t1.idCliente = t2.idCliente
	),
	tb_freq_valor AS (
		SELECT
			IdCliente,
			count(DISTINCT DATE(DtCriacao)) AS qtdeFrequencia,
			sum(
				CASE
					WHEN QtdePontos > 0 THEN QtdePontos
					ELSE 0
				END
			) AS qtdePontosPos
			-- sum(abs(QtdePontos)) as qtdePontosAbs
		FROM
			points_transacoes
		WHERE
			DtCriacao < DATE('{date}')
			AND DATE_DIFF (day, DtCriacao, DATE('{date}')) < 28
		GROUP BY
			idCliente
		ORDER BY
			qtdeFrequencia DESC
	),
	tb_cluster AS (
		SELECT
			*,
			CASE
				WHEN qtdeFrequencia <= 10
				AND qtdePontosPos >= 1500 THEN '12-HYPER'
				WHEN qtdeFrequencia > 10
				AND qtdePontosPos >= 1500 THEN '22-EFICIENTE'
				WHEN qtdeFrequencia <= 10
				AND qtdePontosPos >= 750 THEN '11-INDECISO'
				WHEN qtdeFrequencia > 10
				AND qtdePontosPos >= 750 THEN '21-ESFORÇADO'
				WHEN qtdeFrequencia < 5 THEN '00-LURKER'
				WHEN qtdeFrequencia <= 10 THEN '01-PREGUIÇOSO'
				WHEN qtdeFrequencia > 10 THEN '20-POTENCIAL'
			END AS cluster
		FROM
			tb_freq_valor
	),
	tb_final AS (
		SELECT
			date(date_add(day, -1, DATE('{date}'))) AS dtRef,
			t1.*,
			t2.qtdeFrequencia,
			t2.qtdePontosPos,
			t2.cluster
		FROM
			tb_life_cycle AS t1
			LEFT JOIN tb_cluster AS t2 ON t1.IdCliente = t2.IdCliente
	)

SELECT *
FROM tb_final

"""

df_life_cycle = exec_long_dates(layer="Silver",
                                folder="tmw_ecosys",
                                table_name="life_cycle",
                                query=query_life_cycle,
                                dates=dates)