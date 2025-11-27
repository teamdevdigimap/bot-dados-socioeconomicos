print("Iniciando main.py")
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
    input_func = input("Digite o nome ou número da função a ser executada: \n1. Renda \n2. Habitacao \n3. Emprego \n4. Demografia \n5. Financas \n6. Educacao \n7. Seguranca \n8. Territorio \n9. Todas \n").strip().lower()
    if input_func == "renda" or input_func == "1":
        renda()
    elif input_func == "habitacao" or input_func == "2":
        habitacao()
    elif input_func == "emprego" or input_func == "3":
        emprego()
    elif input_func == "demografia" or input_func == "4":
        demografia()
    elif input_func == "financas" or input_func == "5":
        financas()
    elif input_func == "educacao" or input_func == "6":
        educacao()
    elif input_func == "seguranca" or input_func == "7":
        seguranca()
    elif input_func == "territorio" or input_func == "8":
        territorio()
    elif input_func == "todas" or input_func == "9":
        renda()
        habitacao()
        emprego()
        demografia()
        financas()
        educacao()
        seguranca()
        territorio()
    else:
        logging.error("Função inválida. Por favor, tente novamente.")

# if __name__ == "__main__":
#     territorio()