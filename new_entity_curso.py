from pyspark.sql import functions as f
from pyspark.sql.window import Window
from operators import operators

from models.surrogate.colaborar import sk_id_curso

from utils.depara_turno import DEPARA_TURNO
from utils.depara_modalidade import DEPARA_MODALIDADE

from wrappers.transformation import Transformation
from repository.source.raw.nrt.colaborar.colaborar import (
    c_edcurso,
    c_edmodulo,
    c_edcurhist,
    c_edtpoferta,
    c_edtpcurso)

from repository.target.trusted.entity_curso import (
    entity_curso)


class EntityCurso(Transformation):
    def __init__(self, *args, **kw):
        super().__init__(entity_curso, *args, **kw)

    def read(self):
        col_dt_atlz = f.to_timestamp('DT_ATLZ', 'MM/dd/yyyy HH:mm:ss').alias('DT_ATLZ_ORI')

        self.df_c_edcurso = (
            c_edcurso
            .find_all()
            .select(
                f.col('ECUR_CD'),
                f.col('ECUR_NM'),
                f.col('ECUR_TP_TURNO'),
                f.col('TPCS_CD'),
                f.col('TP_OPER'),
                col_dt_atlz)
            .alias('EDCURSO')
        )

        self.df_c_edmodulo = (
            c_edmodulo
            .find_all()
            .select(
                f.col('ECUR_CD'),
                f.col('MODU_CD'),
                col_dt_atlz)
            .alias('EDMODULO')
        )

        self.df_c_edcurhist = (
            c_edcurhist
            .find_all()
            .select(
                f.col('TPOF_CD'),
                f.col('MODU_CD'),
                col_dt_atlz)
            .alias('EDCURHIST')
        )

        self.df_c_edtpoferta = (
            c_edtpoferta
            .find_all()
            .select(
                f.col('TPOF_DS'),
                f.col('TPOF_CD'),
                col_dt_atlz)
            .alias('EDTPOFERTA')
        )

        self.df_c_edtpcurso = (
            c_edtpcurso
            .find_all()
            .select(
                f.col('TPCS_CD'),
                f.col('TPCS_DS'),
                col_dt_atlz)
            .alias('EDTPCURSO')
        )

    def join(self):

        result_df = (
            self.df_c_edcurso
            .join(
                self.df_c_edtpcurso,
                ['TPCS_CD'],
                "inner")
            .join(
                self.df_c_edmodulo,
                ['ECUR_CD'],
                "inner")
            .join(
                self.df_c_edcurhist,
                ['MODU_CD'],
                "inner")
            .join(
                self.df_c_edtpoferta,
                ['TPOF_CD'],
                "inner")
        )

        return result_df

    def transform(self, df):
        depara_turno = DEPARA_TURNO.copy()
        depara_modalidade = DEPARA_MODALIDADE.copy()
        del depara_turno[None]
        del depara_modalidade[None]

        col_row_number = (
            f.row_number()
            .over(
                Window.partitionBy('CD_CURSO', 'CD_TIPO_OFERTA')
                .orderBy(f.col('EDCURSO.DT_ATLZ_ORI').desc())))

        df = (
            df
            .withColumn('ID_SISTEMA_ORIGEM', operators.id_sistema_origem('colaborar'))
            .withColumn('ID_UNIDADE', f.lit('-2'))
            .withColumn("QT_DURACAO_CURSO", f.lit(None))
            .withColumn("QT_DURACAO_CURSO_SEMESTRE", f.lit(None))
            .withColumn("CD_CURSO_MEC", f.lit(None))
            .withColumn("CD_CONTA_CURSO", f.lit(None))
            .withColumn("NR_CENTRO_CUSTO", f.lit(None))
            .withColumn("DS_TURNO_PADRONIZADO", operators.mapping_dict(depara_turno, "ECUR_TP_TURNO"))
            .withColumn("DS_MODALIDADE_PADRONIZADO", operators.mapping_dict(depara_modalidade, "TPOF_DS"))
            .withColumn("DS_CURSO_MKT", f.lit(None))
            .withColumn("FL_MIGRAR", f.lit(None))

            .withColumn("CD_CURSO", f.col("EDCURSO.ECUR_CD"))
            .withColumn("CD_TIPO_OFERTA", f.col("EDCURHIST.TPOF_CD"))
            .withColumn('ID_CURSO', sk_id_curso())

            .withColumn("CD_CURSO", f.col("ECUR_CD"))
            .withColumn("DS_CURSO", f.col("ECUR_NM"))
            .withColumn("DS_TURNO", f.col("ECUR_TP_TURNO"))
            .withColumn("DS_MODALIDADE", f.col("TPOF_DS"))
            .withColumn("DS_TIPO_CURSO", f.col("TPCS_DS"))

            .withColumn('DT_OPER', f.greatest(
                'EDCURSO.DT_ATLZ_ORI',
                'EDTPCURSO.DT_ATLZ_ORI',
                'EDMODULO.DT_ATLZ_ORI',
                'EDCURHIST.DT_ATLZ_ORI',
                'EDTPOFERTA.DT_ATLZ_ORI',
                ))

            .withColumn('TP_OPER', f.col('EDCURSO.TP_OPER'))
            .withColumn('row_number', col_row_number)
            .filter('row_number==1')
        )

        return df
