import cx_Oracle as db
import petl

from utils import CursorProxy


def open_connection():
    # Estabelece as conexões com os bancos de dados operacional e dimensional
    op_con = db.connect("locadora/locadora@localhost:1521/xe")
    dm_con = db.connect("dw_locadora/dw_locadora@localhost:1521/xe")

    return op_con, dm_con


def get_cursor_dm(dm_con):
    # Retorna o cursor para o banco de dados oracle
    return CursorProxy(dm_con.cursor())


def create_dm_socios(op_con, dm_con):
    # Extrai os dados do banco operacional e insere os dados no banco dimensional
    socios = petl.fromdb(op_con, "select * from socios")
    tipo_socios = petl.fromdb(op_con, "select * from tipos_socios")

    socios_full = petl.transform.join(tipo_socios, socios, key="COD_TPS")
    socios_full = petl.transform.basics.cut(
        socios_full, "COD_SOC", "NOM_SOC", "DSC_TPS"
    )
    dm_socios = petl.rename(socios_full, {"COD_SOC": "ID_SOC", "DSC_TPS": "TIPO_SOCIO"})

    petl.todb(dm_socios, get_cursor_dm(dm_con), "DM_SOCIO")


def create_dm_gravadora(op_con, dm_con):
    # Extrai os dados do banco operacional e insere os dados no banco dimensional
    gravadoras = petl.fromdb(op_con, "select * from gravadoras")
    dm_gravadoras = petl.rename(gravadoras, {"COD_GRAV": "ID_GRAV"})
    petl.todb(dm_gravadoras, get_cursor_dm(dm_con), "DM_GRAVADORA")


if __name__ == "__main__":
    # Obtenção das conexões
    op_con, dm_con = open_connection()

    # Criação das dimensões (Extract, Transform, Load)
    create_dm_socios(op_con, dm_con)
    create_dm_gravadora(op_con, dm_con)
