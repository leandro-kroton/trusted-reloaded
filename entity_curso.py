from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from deparas import (
    map_depara_camcod,
    map_depara_turno,
    map_depara_modalidade
)

from schemas import (
    schema_nrt_o_cursos,
    schema_nrt_o_coordena,
    schema_nrt_o_depara_curso_unidade,
    schema_nrt_o_especial,
)

spark = SparkSession.builder.getOrCreate()

ENTITY_CURSOS = "s3://bucket/trusted/entity_curso"
NRT_O_CURSOS = "s3://bucket/raw/NRT/O_CURSOS"
NRT_O_COORDENA = "s3://bucket/raw/NRT/O_COORDENA"
NRT_SRC_BI_DE_PARA_CURSO_UNIDADE = "s3://bucket/raw/NRT/SRC_BI_DE_PARA_CURSO_UNIDADE"
NRT_O_ESPECIAL = "s3://bucket/raw/NRT/O_ESPECIAL"

df_cursos = (
    spark.read.parquet(NRT_O_CURSOS, schema=schema_nrt_o_cursos)
    .select(
        "CURCOD",
        "CORCOD")
)

df_coordena = (
    spark.read.parquet(NRT_O_COORDENA, schema=schema_nrt_o_coordena)
    .select(
        "CORCOD",
        "CORNOMECHEFE")
)

df_de_para_curso_unidade = (
    spark.read.parquet(NRT_SRC_BI_DE_PARA_CURSO_UNIDADE, schema=schema_nrt_o_depara_curso_unidade)
    .select(
        "CD_UNIDADE",
        "CD_ESPECIALIDADE",
        "DS_TURNO",
        "DS_MODALIDADE",
        "FL_MIGRAR",
        "DT_INSR",
        "DS_ESPECIALIDADE_MKT")
)

df_especial = (
    spark.read.parquet(NRT_O_ESPECIAL, schema=schema_nrt_o_especial)
    .select(
        "ESPCOD",
        "ESPCAMPUS",
        "CURCOD",
        "ESPDESC",
        "ESPTURNO",
        "ESPDURACAO",
        "ESPDURACAOMAXIMA",
        "ESPTIPO",
        "ESPCODMEC",
        "ESPDESCEXIBICAO",
        "SGCONTCODIGOCTB",
        "SGCUSTCODIGOCTB",
        "ESPMODALIDADE",
        "DT_ATLZ"
    )
)

df_entity_curso = (
    df_especial

    .join(
        df_de_para_curso_unidade,
        (
            (df_especial.ESPCAMPUS == df_de_para_curso_unidade.CD_UNIDADE) &
            (df_especial.ESPCOD == df_de_para_curso_unidade.CD_ESPECIALIDADE)
        ),
        'left')

    .join(
        df_cursos,
        df_especial.CURCOD == df_cursos.CURCOD,
        'left')

    .join(
        df_coordena,
        df_cursos.CORCOD == df_coordena.CORCOD,
        'left')

    .withColumn(
        'ID_SISTEMA_ORIGEM', f.lit(2))

    .withColumn(
        'CAMCOD', map_depara_camcod.getItem(f.col("ESPCAMPUS")))

    .select(
        f.md5(f.concat_ws('-', 'ID_SISTEMA_ORIGEM', 'ESPCOD')).cast('string').alias('ID_CURSO'),
        f.col('ESPCOD').cast('string').alias('CD_CURSO'),
        f.col('ESPDESC').cast('string').alias('DS_CURSO'),
        f.col('DS_ESPECIALIDADE_MKT').cast('string').alias('DS_CURSO_MKT'),
        f.col('ESPTURNO').cast('string').alias('DS_TURNO'),
        map_depara_turno.getItem(f.col("ESPTURNO")).cast('string').alias('DS_TURNO_PADRONIZADO'),
        f.col('ESPMODALIDADE').cast('string').alias('DS_MODALIDADE'),
        map_depara_modalidade.getItem(f.col('ESPMODALIDADE')).cast('string').alias('DS_MODALIDADE_PADRONIZADO'),
        f.col('ESPDURACAO').cast('double').alias('QT_DURACAO_CURSO'),
        f.ceil(f.col('ESPDURACAO') / 6).cast('double').alias('QT_DURACAO_CURSO_SEMESTRE'),
        f.col('ESPCODMEC').cast('long').alias('CD_CURSO_MEC'),
        f.col('ESPTIPO').cast('string').alias('DS_TIPO_CURSO'),
        f.col('FL_MIGRAR').cast('integer'),
        f.col('ID_SISTEMA_ORIGEM').cast('long'),
        f.md5(f.concat_ws('-', 'ID_SISTEMA_ORIGEM', 'CAMCOD')).cast('string').alias('ID_UNIDADE'),
        f.col('SGCUSTCODIGOCTB').cast('string').alias('NR_CENTRO_CUSTO'),
        f.col('SGCONTCODIGOCTB').cast('string').alias('CD_CONTA_CURSO'),
        f.col('CAMCOD').cast('string').alias('CD_UNIDADE'),
        f.col('CORNOMECHEFE').cast('string').alias('DS_COORDENADOR_CURSO'),
        f.lit('I').cast('string').alias('TP_OPER'),
        f.to_timestamp('DT_ATLZ', format="MM/dd/yyyy HH:mm:ss.sss").cast('timestamp').alias('DT_OPER'),
        f.lit(0).cast('long').alias('ID_EXECUCAO'),
        f.current_timestamp().alias("DT_ATLZ"),
        f.year(f.current_date()).alias("year"),
        f.month(f.current_date()).alias("month"),
        f.dayofmonth(f.current_date()).alias("day"),
    )
)

(
    df_entity_curso
    .write.partitionBy('ID_SISTEMA_ORIGEM', 'year', 'month', 'day')
    .parquet(ENTITY_CURSOS)
)
