from datetime import datetime
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


def create_dm_artista(op_con, dm_con):

    artistas = petl.fromdb(op_con, "select * from artistas")
    dm_artistas = petl.transform.basics.cut(
        artistas, "COD_ART", "TPO_ART", "NAC_BRAS", "NOM_ART"
    )

    dm_artistas = petl.rename(dm_artistas, {"COD_ART": "ID_ART"})
    petl.todb(dm_artistas, get_cursor_dm(dm_con), "DM_ARTISTA")


def create_dm_titulo(op_con, dm_con):

    titulos = petl.fromdb(op_con, "select * from titulos")

    dm_titulos = petl.transform.basics.cut(
        titulos, "COD_TIT", "TPO_TIT", "CLA_TIT", "DSC_TIT"
    )

    dm_titulos = petl.rename(
        dm_titulos,
        {
            "COD_TIT": "ID_TITULO",
            "TPO_TIT": "TPO_TITULO",
            "CLA_TIT": "CLA_TITULO",
            "DSC_TIT": "DSC_TITULO",
        },
    )

    petl.todb(dm_titulos, get_cursor_dm(dm_con), "DM_TITULO")


def create_dm_tempo(op_con, dm_con):
    nomes_mes = {
        1: "Janeiro",
        2: "Fevereiro",
        3: "Março",
        4: "Abril",
        5: "Maio",
        6: "Junho",
        7: "Julho",
        8: "Agosto",
        9: "Setembro",
        10: "Outubro",
        11: "Novembro",
        12: "Dezembro",
    }

    siglas_mes = {
        1: "JAN",
        2: "FEV",
        3: "MAR",
        4: "ABR",
        5: "MAI",
        6: "JUN",
        7: "JUL",
        8: "AGO",
        9: "SET",
        10: "OUT",
        11: "NOV",
        12: "DEZ",
    }
    tempo = petl.fromdb(op_con, "select distinct dat_loc from locacoes")

    id_tempo = []
    nu_ano = []
    nu_mes = []
    nu_dia = []
    nu_anomes = []
    nm_mesano = []
    sg_mes = []
    nm_mes = []
    nu_hora = []
    turno = []

    for t in tempo:
        if t[0] != "DAT_LOC":
            date_tempo = t[0].strftime("%y%m%d")
            id_tempo.append(int(date_tempo))
            nu_ano.append(int(t[0].year))
            nu_mes.append(int(t[0].month))
            nu_dia.append(int(t[0].day))
            nu_anomes.append(int(f"{t[0].year}{t[0].month}"))
            nm_mesano.append(f"{siglas_mes[t[0].month]} {t[0].year}")
            sg_mes.append(siglas_mes[t[0].month])
            nm_mes.append(nomes_mes[t[0].month])
            nu_hora.append(t[0].hour)
            turno.append("Desconhecido")

    dm_tempo = petl.rename(tempo, {"DAT_LOC": "DT_TEMPO"})
    dm_tempo = petl.transform.basics.addcolumn(dm_tempo, "ID_TEMPO", id_tempo)
    dm_tempo = petl.transform.basics.addcolumn(dm_tempo, "NU_ANO", nu_ano)
    dm_tempo = petl.transform.basics.addcolumn(dm_tempo, "NU_MES", nu_mes)
    dm_tempo = petl.transform.basics.addcolumn(dm_tempo, "NU_DIA", nu_dia)
    dm_tempo = petl.transform.basics.addcolumn(dm_tempo, "NU_ANOMES", nu_anomes)
    dm_tempo = petl.transform.basics.addcolumn(dm_tempo, "NM_MESANO", nm_mesano)
    dm_tempo = petl.transform.basics.addcolumn(dm_tempo, "SG_MES", sg_mes)
    dm_tempo = petl.transform.basics.addcolumn(dm_tempo, "NM_MES", nm_mes)
    dm_tempo = petl.transform.basics.addcolumn(dm_tempo, "NU_HORA", nu_hora)
    dm_tempo = petl.transform.basics.addcolumn(dm_tempo, "TURNO", turno)

    petl.todb(dm_tempo, get_cursor_dm(dm_con), "DM_TEMPO")


if __name__ == "__main__":
    # Obtenção das conexões
    op_con, dm_con = open_connection()

    # Criação das dimensões (Extract, Transform, Load)
    create_dm_socios(op_con, dm_con)
    create_dm_gravadora(op_con, dm_con)
    create_dm_artista(op_con, dm_con)
    create_dm_titulo(op_con, dm_con)
    create_dm_tempo(op_con, dm_con)
