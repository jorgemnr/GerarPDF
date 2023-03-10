from conexao_oracle import conexao_oracle
from logger import logger
from threading import Thread, Semaphore, current_thread
from datetime import datetime
import subprocess
import os


class geracao_PDF:
    def __init__(self, ambiente, qtdeThreads):
        ############################################
        # PARAMETRIZAÇÃO
        ############################################

        ############################################
        # HOMOLOGAÇÃO
        ############################################
        if ambiente == "hom":
            self.database = "sta"
            self.motor_reports = "C:\orant\BIN\RWRUN60.EXE"
            self.user_id = "USERID=usertrac/trac840820.@c0828sta"
            self.caminho_relatorio = "MODULE=//c0828/obj_pc/usr/procger/CCME/"

        ############################################
        # PRODUÇÃO
        ############################################
        elif ambiente == "prd":
            self.database = "prd"
            self.motor_reports = "C:\orant\BIN\RWRUN60.EXE"
            self.user_id = "USERID=usertrac/trac840820.@c0090prd"
            self.caminho_relatorio = "MODULE=//c0090/obj_pc/usr/procger/CCME/"

        ############################################
        # PARAMETROS GERAIS
        ############################################
        self.parametros_fixos = "DESFORMAT=PDF DESTYPE=FILE MODE=BITMAP PARAMFORM=NO ORACLE_SHUTDOWN=YES PRINTJOB=NO BATCH=YES"
        # Quantidade de THREADS em paralelo
        self.semaforo = Semaphore(qtdeThreads)

        ############################################
        # CONECTAR ORACLE
        ############################################
        self.oracle = conexao_oracle(self.database)

    def gerar_arquivos_PDF(
        self, result_arq=(), ret_arquivo=[], indice=0, tipo_fila=0, tp_impressao=None
    ):
        # LOOP POR ARQUIVO
        logger.info(
            f"{tp_impressao} - processo: {result_arq[2]}, sequencia: {result_arq[0]}, relatorio: {result_arq[3]}"
        )
        try:
            executa_novamente = 1
            while executa_novamente > 0:
                # ds_relatorio = 'MODULE=//c0828/obj_pc/usr/procger/CCME/' + result_arq[3] + '.rep'
                ds_relatorio = self.caminho_relatorio + result_arq[3] + ".rep"
                ds_parametros_relatorio = result_arq[4]
                ds_arquivo_PDF = "DESNAME=" + result_arq[5]
                ds_erro_file = "ERRFILE=" + result_arq[5][:-4] + ".txt"
                #
                comando_pdf = self.motor_reports + " "
                comando_pdf += ds_relatorio + " "
                comando_pdf += ds_parametros_relatorio + " "
                comando_pdf += ds_arquivo_PDF + " "
                comando_pdf += ds_erro_file + " "
                comando_pdf += self.user_id + " "
                comando_pdf += self.parametros_fixos + " "

                result_sub = subprocess.run(
                    comando_pdf
                    # ,capture_output=True
                    # ,text=True
                    ,
                    timeout=60
                    # ,input=b"underwater"
                )
                # print("stdout:", result.stdout)
                # print("stderr:", result.stderr)

                # Analisar execução com sucesso/erro
                # Pegar o retorno da execução 0-Ok, X-Erros
                # Se diferente de zero então é erro
                ret_arquivo[indice] = result_sub.returncode

                # Pegar conteúdo do arquivo se returncode diferente de zero
                nm_arquivo = result_arq[5][:-4] + ".txt"
                arquivo_inteiro = None
                erro = None
                existe_erro = 0
                # ret_arquivo[indice] = 1 - teste de erro
                if ret_arquivo[indice] != 0:
                    try:
                        arquivo = open(nm_arquivo, "r")
                        arquivo_inteiro = arquivo.read()
                        arquivo.close()
                        erro = (
                            arquivo_inteiro
                            + ", ReturnCode: "
                            + str(ret_arquivo[indice])
                        )
                    except Exception as Erro:
                        existe_erro = 1
                        erro = str(Erro) + ", ReturnCode: " + str(ret_arquivo[indice])
                        erro = f"{tp_impressao} - Ler arquivo log reports - processo: {result_arq[2]}, sequencia: {result_arq[0]}, relatorio: {result_arq[3]}, erro: {erro}, Comando PDF: {comando_pdf}"
                        logger.error(erro)

                    # Mudar status fila geração PDF e guardar conteúdo arquivo
                    self.oracle.prc_processar_fila_pdf(
                        p_cd_sequencia=result_arq[0],
                        p_cd_tipo_fila=tipo_fila,
                        p_id_status=0,
                        p_cd_sequencia_sub=result_arq[1],
                        p_ds_log=erro,
                    )
                    logger.error(
                        f"{tp_impressao} - Erro gerar PDF - processo: {result_arq[2]}, sequencia: {result_arq[0]}, relatorio: {result_arq[3]}, arquivo: {result_arq[5][:-4] + '.pdf'}"
                    )

                # excluir arquivo de log do reports
                try:
                    if existe_erro == 0:
                        os.remove(nm_arquivo)
                except Exception as Erro:
                    # Mudar status fila geração PDF e guardar conteúdo arquivo
                    ret_arquivo[indice] = 27
                    erro = f"{tp_impressao} - Erro remover arquivo log reports - processo: {result_arq[2]}, sequencia: {result_arq[0]}, relatorio: {result_arq[3]}, arquivo: {nm_arquivo}, erro: {str(Erro)}"
                    logger.error(erro)
                    self.oracle.prc_processar_fila_pdf(
                        p_cd_sequencia=result_arq[0],
                        p_cd_tipo_fila=tipo_fila,
                        p_id_status=3,
                        p_cd_sequencia_sub=result_arq[1],
                        p_ds_log=erro,
                    )

                # TENTAR NOVAMENTE QUANDO RETURNCODE
                if ret_arquivo[indice] == 3221225477 and executa_novamente == 1:
                    executa_novamente = 2
                    logger.info(
                        f"{tp_impressao} - processo: {result_arq[2]}, sequencia: {result_arq[0]}, relatorio: {result_arq[3]} - NOVA TENTATIVA"
                    )
                else:
                    executa_novamente = 0

        except Exception as Erro:
            msg = f"{tp_impressao} - Erro geral - processo: {result_arq[2]}, sequencia: {result_arq[0]}, relatorio: {result_arq[3]} erro: {str(Erro)}"
            logger.error(msg)
            ret_arquivo[indice] = 99
            self.oracle.prc_processar_fila_pdf(
                p_cd_sequencia=result_arq[0],
                p_cd_tipo_fila=tipo_fila,
                p_id_status=0,
                p_cd_sequencia_sub=result_arq[1],
                p_ds_log=msg,
            )

    def impressao_automatica(self, result_proc=[]):
        result_arquivos = []
        try:
            # raise Exception("erro teste")
            result_arquivos = self.oracle.ler_fila(
                cd_tipo_fila=1, result_proc=result_proc
            )
        except Exception as Erro:
            msg = f"impressao_automatica - ler_fila - processo: {result_proc[1]}, sequencia: {result_proc[0]}, Erro: {str(Erro)}"
            logger.error(msg)

        if len(result_arquivos) != 0:
            logger.info(f"processo: {result_proc[1]}, sequencia: {result_proc[0]}")

            # Vetor de retorno para threads
            ret_arquivos = [0] * 10
            indice = -1
            # Gerar cada arquivo em um Thread diferente
            threads = list()
            for result_arq in result_arquivos:
                indice += 1
                t = Thread(
                    target=self.gerar_arquivos_PDF,
                    args=(result_arq, ret_arquivos, indice, 1, "impressao_automatica"),
                )
                threads.append(t)
                t.start()

            for index, thread in enumerate(threads):
                thread.join()

            # verificar se houve erro no processamento de algum arquivo
            v_id_status = 1
            for i in ret_arquivos:
                if i != 0:
                    v_id_status = 3

            # Mudar status fila geração PDF
            self.oracle.prc_processar_fila_pdf(
                p_cd_sequencia=result_arq[0],
                p_cd_tipo_fila=1,
                p_id_status=v_id_status,
                p_cd_sequencia_sub=None,
                p_ds_log=None,
            )

    def shipment_details(self, result_proc=[]):
        result_arquivos = []
        try:
            # raise Exception("erro teste")
            result_arquivos = self.oracle.ler_fila(
                cd_tipo_fila=2, result_proc=result_proc
            )
        except Exception as Erro:
            msg = f"shipment_details - ler_fila - processo: {result_proc[1]}, sequencia: {result_proc[0]}, Erro: {str(Erro)}"
            logger.error(msg)
            # return

        if len(result_arquivos) != 0:
            logger.info(f"processo: {result_proc[1]}, sequencia: {result_proc[0]}")
            # Gerar cada arquivo em um Thread diferente
            threads = list()
            ret_arquivos = [0] * 10
            indice = -1
            for result_arq in result_arquivos:
                indice += 1
                t = Thread(
                    target=self.gerar_arquivos_PDF,
                    args=(result_arq, ret_arquivos, indice, 2, "shipment_details"),
                )
                threads.append(t)
                t.start()

            for index, thread in enumerate(threads):
                thread.join()

            # verificar se houve erro no processamento de algum arquivo
            v_id_status = 1
            for i in ret_arquivos:
                if i != 0:
                    v_id_status = 3
            # Mudar status fila geração PDF
            self.oracle.prc_processar_fila_pdf(
                p_cd_sequencia=result_arq[0],
                p_cd_tipo_fila=2,
                p_id_status=v_id_status,
                p_cd_sequencia_sub=None,
                p_ds_log=None,
            )

    def executar_processo(self, result_proc=[]):
        # logger.info(f"INICIO: {current_thread().name}")
        with self.semaforo:
            # IMPRESSAO AUTOMATICA
            # threads = list()
            # t = Thread(
            #     target=self.impressao_automatica,
            #     args=(result_proc,),
            # )
            # threads.append(t)
            # t.start()
            self.impressao_automatica(result_proc)

            # SHIPMENT DETAILS
            # tt = Thread(
            #     target=self.shipment_details,
            #     args=(result_proc,),
            # )
            # threads.append(tt)
            # tt.start()
            self.shipment_details(result_proc)

            # AGUARDAR FINALIZAÇÃO DAS THREADS
            # for index, thread in enumerate(threads):
            #     thread.join()

    def executar(self):
        # LOOP POR SEQUENCIA/PROCESSO
        result_processos = self.oracle.ler_fila_processos()
        if len(result_processos) == 0:
            logger.info("Não existem processos na Fila")
            return

        # Gerar cada processo em um Thread diferente
        threads = list()
        for result_proc in result_processos:
            t = Thread(
                target=self.executar_processo,
                args=(result_proc,),
            )
            threads.append(t)
            t.name = f"<THREAD {len(threads)}> Sequencia: {result_proc[0]} Processo: {result_proc[1]}"
            logger.info(t.name)
            t.start()

        for index, thread in enumerate(threads):
            thread.join()


if __name__ == "__main__":
    ############################################
    # AMBIENTE EXECUÇÃO
    ############################################
    ambiente = "hom"
    qtdeThreads = 3

    ############################################
    # Data e hora - INICIO
    ############################################
    agora = datetime.now()  # current date and time
    data_com_hora = agora.strftime("%d/%m/%Y, %H:%M:%S")
    logger.info(f"======= <INICIO> - Data: {data_com_hora} =======")

    ############################################
    # EXECUTAR PROCESSAMENTO
    ############################################
    gerar_PDF = geracao_PDF(ambiente, qtdeThreads)
    gerar_PDF.executar()

    ############################################
    # Data e hora - FINAL
    ############################################
    agora = datetime.now()  # current date and time
    data_com_hora = agora.strftime("%d/%m/%Y, %H:%M:%S")
    logger.info(f"======= <FIM> - Data: {data_com_hora} =======")
