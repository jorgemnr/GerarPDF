import cx_Oracle
from logger import logger


class conexao_oracle:
    def __init__(self, database="sta"):
        self.database = database
        self.con = None

    def connect(self):
        try:
            if self.database == "dvm":
                self.con = cx_Oracle.connect("user/senha@banco")
            elif self.database == "sta":
                self.con = cx_Oracle.connect("user/senha@banco")
            elif self.database == "prd":
                self.con = cx_Oracle.connect("user/senha@banco")
        except Exception as e:
            logger.error(f"Erro connect oracle - database: {self.database} erro: {e}")
            # self.send_email(
            #     "CCME",
            #     "CCME1901 - CCME GERAR PDF",
            #     "Problemas Connect Database " + str(e),
            # )
            raise

    def send_email(self, procGer, assunto, mensagemErro):
        self.procGer = procGer
        self.assunto = assunto
        self.mensagemErro = mensagemErro

        try:
            self.connect()
            cur = self.con.cursor()
            cur.callproc(
                "CCME2614.ENVIA_ERRO", [self.procGer, self.assunto, self.mensagemErro]
            )
            cur.close()
        except Exception as e:
            cur.close()
            print("Problem in Send Email : " + str(e))

    def prc_processar_fila_pdf(
        self,
        p_cd_sequencia,
        p_cd_tipo_fila,
        p_id_status,
        p_cd_sequencia_sub=None,
        p_ds_log=None,
    ):
        try:
            self.connect()
            cur = self.con.cursor()
            cur.callproc(
                "ccme1901.prc_processar_fila_pdf",
                [
                    p_cd_sequencia,
                    p_cd_tipo_fila,
                    p_id_status,
                    p_cd_sequencia_sub,
                    p_ds_log,
                ],
            )
            cur.close()
        except Exception as e:
            # cur.close()
            msg = (
                "ccme1901.prc_processar_fila_pdf - sequencia : "
                + str(p_cd_sequencia)
                + ", cd_tipo_fila: "
                + str(p_cd_tipo_fila)
                + ", Erro: "
                + str(e)
            )
            logger.error(msg)
            # self.send_email(
            #     "CCME",
            #     "CCME1901 - CCME GERAR PDF",
            #     msg,
            # )
            # raise

    def ler_fila_processos(self):
        self.connect()
        sql = "SELECT t.cd_sequencia\
                      ,t.cd_processo_exportacao\
                      ,COUNT(1) qtde\
                 FROM controle_impressao_att_pdf t\
                WHERE t.id_status = {}\
             GROUP BY t.cd_sequencia\
                      ,t.cd_processo_exportacao\
             ORDER BY t.cd_sequencia".format(
            0
        )
        try:
            cur = self.con.cursor()
            cur.execute(sql)
            fetch_cursor = cur.fetchall()
            cur.close()
            return fetch_cursor
        except Exception as e:
            cur.close()
            logger.error(f"ler_fila_processos - Erro: {e}")
            # self.send_email("CCME", "ler_fila_processos", "Erro: " + str(e))
            raise

    def ler_fila(self, cd_tipo_fila=None, result_proc=()):
        self.connect()
        sql = "SELECT t.cd_sequencia\
                        ,t.cd_sequencia_sub\
                        ,t.cd_processo_exportacao\
                        ,t.ds_relatorio\
                        ,t.ds_parametros_relatorio\
                        ,t.ds_arquivo_pdf\
                FROM controle_impressao_att_pdf t\
                WHERE t.id_status = {}\
                    AND t.cd_tipo_fila = {}\
                    AND t.cd_sequencia = {}\
            ORDER BY t.cd_sequencia\
                    ,t.cd_sequencia_sub".format(
            0, cd_tipo_fila, result_proc[0]
        )
        try:
            cur = self.con.cursor()
            cur.execute(sql)
            fetch_cursor = cur.fetchall()
            cur.close()
            return fetch_cursor
        except Exception as e:
            cur.close()
            msg = (
                "ler_fila SQL - processo: "
                + str(result_proc[1])
                + ", sequencia: "
                + str(result_proc[0])
                + ", cd_tipo_fila: "
                + str(cd_tipo_fila)
                + ", Erro: "
                + str(e)
            )
            # print(msg)
            logger.error(msg)
            # self.send_email(
            #     "CCME",
            #     "CCME1901 - CCME GERAR PDF",
            #     msg,
            # )
            raise
