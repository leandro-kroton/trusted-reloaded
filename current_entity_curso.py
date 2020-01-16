from pyspark.sql.functions import udf, col
from pyspark.sql.types import LongType, DoubleType, StringType, TimestampType, IntegerType

from repository.source.raw.nrt.olimpo.o_coordena_source_repository import OCoordenaSourceRepository
from repository.source.raw.nrt.olimpo.o_curso_source_repository import OCursoSourceRepository
from repository.source.raw.nrt.src_stg_bi_de_para_curso_unidade_source_repository import \
    SrcStgBiDeParaCursoUnidadeSourceRepository
from transformation.base.base_dw_transformation import BaseDWTransformation
from log_config import LogConfig
from transformation.base.base_transformation import get_dt_oper
from utils.depara_camcod import depara_camcod
from models.timestamp_parser import default_timestamp_parser
from cleansing.empty_string_cleansing import empty_string_cleansing
from repository.target.entity.entity_cursos_repository import EntityCursoTargetRepository
from operators.curso_operator import CursoOperator
from operators.unidade_operator import UnidadeOperator
from utils.depara_turno import depara_turno
from utils.depara_modalidade import depara_modalidade
from repository.source.raw.nrt.olimpo.o_especial_source_repository import OEspecialSourceRepository
import math

logger = LogConfig.get_logger(__name__)


class EntityCursoTransformation(BaseDWTransformation):

    def __init__(self, id_execucao):
        self.id_execucao = id_execucao
        self.curso_operator = CursoOperator()
        self.unidade_operator = UnidadeOperator()
        super().__init__(EntityCursoTargetRepository(), id_execucao)

    def select_especial_fields(self, df_especial):
        logger.info("Selecting Curso fields ...........")
        df_especial = df_especial.select(df_especial.ESPCOD,
                                         df_especial.ESPDESC,
                                         df_especial.ESPDESCEXIBICAO,
                                         df_especial.ESPTURNO,
                                         df_especial.ESPMODALIDADE,
                                         df_especial.ESPDURACAO,
                                         df_especial.ESPDURACAOMAXIMA,
                                         df_especial.ESPCODMEC,
                                         df_especial.ESPTIPO,
                                         df_especial.ESPCAMPUS,
                                         df_especial.SIST_ORIG,
                                         df_especial.SGCUSTCODIGOCTB,
                                         df_especial.SGCONTCODIGOCTB,
                                         df_especial.TP_OPER,
                                         df_especial.CURCOD,
                                         default_timestamp_parser(df_especial.DT_ATLZ).alias("DT_ATLZ"))

        return df_especial

    def join_depara(self, df_especial, df_depara):
        logger.info("Adding Depara fields ...........")

        columns = df_especial.columns

        join_key = [df_especial.ESPCAMPUS == df_depara.CD_UNIDADE,
                    df_especial.ESPCOD == df_depara.CD_ESPECIALIDADE]

        df_especial = df_especial.join(df_depara, join_key, "left") \
            .select(*columns,
                    df_depara.DS_ESPECIALIDADE_MKT,
                    df_depara.FL_MIGRAR,
                    get_dt_oper(default_timestamp_parser(df_especial.DT_ATLZ),
                                default_timestamp_parser(
                                    df_depara.DT_INSR)).alias(
                        "DT_OPER"))

        return df_especial

    def join_curso(self, df, df_curso):

        df_curso = df_curso.withColumnRenamed("CURCOD", "CURCOD_")

        join_key = [df.CURCOD == df_curso.CURCOD_]

        return df.join(df_curso, join_key, 'left')\
            .select(*df,
                    df_curso.CORCOD)

    def join_coordenador(self, df, df_coordena):
        df_coordena = df_coordena.withColumnRenamed("CORCOD", "CORCOD_")

        join_key = [df.CORCOD == df_coordena.CORCOD_]

        return df.join(df_coordena, join_key, 'left') \
            .select(*df,
                    df_coordena.CORNOMECHEFE)

    def rename_campus_field(self, df_especial):
        logger.info("Renaming ESPCAMPUS to CAMCOD ...........")

        df_especial = (
            df_especial
            .withColumn("CAMCOD", depara_camcod("ESPCAMPUS"))
        )

        return df_especial

    def set_calculated_fields(self, df_especial):
        logger.info("Adding calculated fields ...........")
        udf_quantidade_semestre = udf(lambda x: math.ceil(float(x) / 6))

        df_especial = df_especial.withColumn("QT_DURACAO_CURSO", col("ESPDURACAO").cast(DoubleType())) \
            .withColumn("QT_DURACAO_CURSO_SEMESTRE", udf_quantidade_semestre("ESPDURACAO").cast(DoubleType())) \
            .withColumn("ESPTURNO", empty_string_cleansing("ESPTURNO")) \
            .withColumn("ESPCODMEC", col("ESPCODMEC").cast(LongType()))

        return df_especial

    def select_result_fields(self, df_especial):
        logger.info("Building output dataframe ...........")
        df_especial = df_especial.select(
            df_especial.ID_CURSO.cast(StringType()),
            df_especial.CD_CURSO.cast(StringType()),
            df_especial.ESPDESC.alias("DS_CURSO").cast(StringType()),
            df_especial.DS_ESPECIALIDADE_MKT.alias("DS_CURSO_MKT").cast(StringType()),
            df_especial.ESPTURNO.alias("DS_TURNO").cast(StringType()),
            df_especial.DS_TURNO_PADRONIZADO.cast(StringType()),
            df_especial.ESPMODALIDADE.alias("DS_MODALIDADE").cast(StringType()),
            df_especial.DS_MODALIDADE_PADRONIZADO.cast(StringType()),
            df_especial.QT_DURACAO_CURSO.cast(DoubleType()),
            df_especial.QT_DURACAO_CURSO_SEMESTRE.cast(DoubleType()),
            df_especial.ESPCODMEC.alias("CD_CURSO_MEC").cast(LongType()),
            df_especial.ESPTIPO.alias("DS_TIPO_CURSO").cast(StringType()),
            df_especial.FL_MIGRAR.cast(IntegerType()),
            df_especial.ID_SISTEMA_ORIGEM.cast(LongType()),
            df_especial.ID_UNIDADE.cast(StringType()),
            df_especial.SGCUSTCODIGOCTB.alias("NR_CENTRO_CUSTO").cast(StringType()),
            df_especial.SGCONTCODIGOCTB.alias("CD_CONTA_CURSO").cast(StringType()),
            df_especial.CAMCOD.alias("CD_UNIDADE").cast(StringType()),
            df_especial.CORNOMECHEFE.alias("DS_COORDENADOR_CURSO").cast(StringType()),
            df_especial.TP_OPER.cast(StringType()),
            df_especial.DT_OPER.cast(TimestampType())
        )

        return df_especial

    def depara_padronizado(self, df_especial):
        logger.info("Create DS_TURNO_PADRONIZADO, DS_MODALIDADE_PADRONIZADO ...........")

        df_especial = df_especial.withColumn("DS_TURNO_PADRONIZADO", depara_turno("ESPTURNO")) \
            .withColumn("DS_MODALIDADE_PADRONIZADO", depara_modalidade("ESPMODALIDADE"))

        return df_especial

    def execute_validator(self, class_name):

        self.validador.execute(self.df_especial, OEspecialSourceRepository().source_path, class_name)

        self.validador.execute(
            self.df_de_para_curso_unidade,
            SrcStgBiDeParaCursoUnidadeSourceRepository().source_path,
            class_name)

        self.validador.execute(self.df_result, self.repository.output_path, class_name)

    def execute(self, path):
        """
            This is the 'main' transformation Entity Curso function that applies all the above methods to the
            dataframe. After transformation, save the final dataframe in the class, and also save to s3
        """
        logger.info("Starting transformation Entity Curso")
        logger.info(".......................................")

        logger.info('Reading repositories')

        self.df_de_para_curso_unidade = SrcStgBiDeParaCursoUnidadeSourceRepository().find_all()
        self.df_especial = OEspecialSourceRepository().find_all(path)
        df_curso = OCursoSourceRepository().get_most_recent_registers()
        df_coordena = OCoordenaSourceRepository().get_most_recent_registers()

        logger.info('Finished reading repositories')

        df_result = self.select_especial_fields(self.df_especial)
        df_result = self.join_depara(df_result, self.df_de_para_curso_unidade)
        df_result = self.join_curso(df_result, df_curso)
        df_result = self.join_coordenador(df_result, df_coordena)
        df_result = self.rename_campus_field(df_result)
        df_result = self.depara_padronizado(df_result)
        df_result = self.set_id_sistema_origem(df_result)
        df_result = self.unidade_operator.set_id_unidade(df_result)
        df_result = self.curso_operator.set_id_curso(df_result)
        df_result = self.set_calculated_fields(df_result)
        df_result = self.select_result_fields(df_result)

        logger.info("Finishing transformation Entity Curso")
        logger.info(".......................................")

        self.df_result = df_result

        logger.info('Saving to S3')
        self.save()

        # Report Validator
        self.execute_validator(self.__class__.__name__)

        return df_result
