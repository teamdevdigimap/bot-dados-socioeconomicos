
from Renda import renda_run
from Habitacao import habitacao_run
from Emprego import emprego_run
#from Saude import saude_run
from Territorio import territorio_run
from Demografia import demografia_run
from Financas import financa_run
from Educacao import educacao_run
from Seguranca import seguranca_run
import logging

logging.basicConfig(level=logging.INFO)

def renda():
    logging.info("Começa renda")
    renda_run()
    logging.info("Terminou renda")

def habitacao():
    logging.info("Começa habitação")
    habitacao_run()
    logging.info("Terminou habitação")

def emprego():
    logging.info("Começa emprego")
    emprego_run()
    logging.info("Terminou emprego")

def demografia():
    logging.info("Começa demografia")
    demografia_run()
    logging.info("Terminou demografia")

def financas():
    logging.info("Começa finanças")
    financa_run()
    logging.info("Terminou finanças")

# def saude():
#     logging.info("Começa saúde")
#     saude_run()
#     logging.info("Terminou saúde")

def educacao():
    logging.info("Começa educação")
    educacao_run()
    logging.info("Terminou educação")

def seguranca():
    logging.info("Começa segurança")
    seguranca_run()
    logging.info("Terminou segurança")

def territorio():
    logging.info("Começa território")
    territorio_run()
    logging.info("Terminou território")

if __name__ == "__main__":
    territorio()