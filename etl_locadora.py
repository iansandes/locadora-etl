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
    # Extrai os dados do banco operacional e insere os dados no banco dimensional

    artistas = petl.fromdb(op_con, "select * from artistas")
    dm_artistas = petl.transform.basics.cut(
        artistas, "COD_ART", "TPO_ART", "NAC_BRAS", "NOM_ART"
    )

    dm_artistas = petl.rename(dm_artistas, {"COD_ART": "ID_ART"})
    petl.todb(dm_artistas, get_cursor_dm(dm_con), "DM_ARTISTA")


def create_dm_titulo(op_con, dm_con):
    # Extrai os dados do banco operacional e insere os dados no banco dimensional

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

    # Estruturas de meses auxiliares para a transformação dos dados
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

    # Extrai os dados do banco operacional e insere os dados no banco dimensional
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


def calcular_multa_tempo(titulos):
    tempo_acumulado = 0
    multa = 0
    artista = 0
    gravadora = 0
    data_tempo = 0

    for titulo in titulos:
        artista = titulo[3]
        gravadora = titulo[1]
        data_tempo = titulo[4]
        if titulo[7] < datetime.now() and titulo[8] != "P":
            timedelta = datetime.now() - titulo[7]
            tempo_acumulado += timedelta.days

            if timedelta.days > 1:
                multa = ((titulo[5] * 2) * 1.03) * timedelta.days
            elif timedelta.days == 1:
                multa = titulo[5] * 2

    return artista, gravadora, tempo_acumulado, multa, data_tempo


def create_ft_locacoes(op_con, dm_con):
    # Extrai os dados do banco operacional e insere os dados no banco dimensional

    locacoes = petl.fromdb(
        op_con,
        """select l.cod_soc, a2.cod_grav, t.cod_tit, t.cod_art, l.dat_loc, l.val_loc, l.dat_pgto, l.dat_venc, l.sta_pgto 
                         from locacoes l
                         join itens_locacoes il on l.cod_soc = il.cod_soc and l.dat_loc = il.dat_loc
                         join copias c2 on il.cod_tit = c2.cod_tit and il.num_cop = c2.num_cop
                         join titulos t on c2.cod_tit = t.cod_tit
                         join artistas a2 on t.cod_art = a2.cod_art""",
    )

    ids_socios = petl.fromdb(op_con, "select cod_soc from socios")
    ids_titulo = petl.fromdb(op_con, "select cod_tit from titulos")
    full_locacoes = petl.transform.joins.crossjoin(ids_socios, ids_titulo)
    ft_locacoes = petl.empty()
    ft_socio = []
    ft_titulo = []
    ft_valor_arrecadado = []
    ft_tempo_devolucao = []
    ft_multa = []
    ft_artista = []
    ft_gravadora = []
    ft_tempo = []

    # Transformação dos dados para o carregamento no banco de dados
    for ft_locacao in full_locacoes:
        valor_arrecadado = []
        calcular_titulos = []
        for locacao in locacoes:
            if ft_locacao[0] == locacao[0] and ft_locacao[1] == locacao[2]:
                if locacao[5] != "VAL_LOC":
                    valor_arrecadado.append(locacao[5])
                if locacao[7] != "DAT_VENC":
                    calcular_titulos.append(locacao)

        if calcular_titulos:
            if ft_locacao[0] == "COD_SOC":
                pass
            else:
                ft_valor_arrecadado.append(sum(valor_arrecadado))
                artista, gravadora, tempo, multa, data_tempo = calcular_multa_tempo(
                    calcular_titulos
                )
                ft_socio.append(ft_locacao[0])
                ft_titulo.append(ft_locacao[1])
                ft_artista.append(artista)
                ft_gravadora.append(gravadora)
                ft_multa.append(multa)
                ft_tempo_devolucao.append(tempo)

                if data_tempo != 0:
                    data_tempo = int(data_tempo.strftime("%y%m%d"))
                    id_tempo = petl.fromdb(
                        dm_con,
                        f"select id_tempo from dm_tempo where id_tempo = {data_tempo}",
                    )
                    ft_tempo.append(id_tempo[1][0])
                else:
                    pass

    ft_locacoes = petl.transform.basics.addcolumn(
        ft_locacoes, "VALOR_ARRECADADO", ft_valor_arrecadado
    )
    ft_locacoes = petl.transform.basics.addcolumn(
        ft_locacoes, "TEMPO_DEVOLUCAO", ft_tempo_devolucao
    )
    ft_locacoes = petl.transform.basics.addcolumn(ft_locacoes, "MULTA_ATRASO", ft_multa)
    ft_locacoes = petl.transform.basics.addcolumn(ft_locacoes, "ID_ART", ft_artista)
    ft_locacoes = petl.transform.basics.addcolumn(ft_locacoes, "ID_GRAV", ft_gravadora)
    ft_locacoes = petl.transform.basics.addcolumn(ft_locacoes, "ID_TEMPO", ft_tempo)
    ft_locacoes = petl.transform.basics.addcolumn(ft_locacoes, "ID_SOC", ft_socio)
    ft_locacoes = petl.transform.basics.addcolumn(ft_locacoes, "ID_TITULO", ft_titulo)

    petl.todb(ft_locacoes, get_cursor_dm(dm_con), "FT_LOCACOES")


if __name__ == "__main__":
    # Obtenção das conexões
    op_con, dm_con = open_connection()

    # Apaga dados da tabela de fatos
    dm_cursor = get_cursor_dm(dm_con)
    dm_cursor.execute("delete from ft_locacoes")
    # Criação das dimensões e fatos (Extract, Transform, Load)
    create_dm_socios(op_con, dm_con)
    create_dm_gravadora(op_con, dm_con)
    create_dm_artista(op_con, dm_con)
    create_dm_titulo(op_con, dm_con)
    create_dm_tempo(op_con, dm_con)
    create_ft_locacoes(op_con, dm_con)
