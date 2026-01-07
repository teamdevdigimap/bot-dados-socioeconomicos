print("Iniciando Atualizações ...\n\n")
print("Importando módulos de renda...\n")
from Renda import renda_run
print("Importando módulos de habitação...\n")
from Habitacao import habitacao_run
print("Importando módulos de emprego...\n")
from Emprego import emprego_run
# print("Importando módulos de saúde...\n")
# from Saude import saude_run
print("Importando módulos de território...\n")
from Territorio import territorio_run
print("Importando módulos de demografia...\n")
from Demografia import demografia_run
print("Importando módulos de finanças...\n")
from Financas import financa_run
print("Importando módulos de educação...\n")
from Educacao import educacao_run
print("Importando módulos de segurança...\n")
from Seguranca import seguranca_run
print("Importando módulos de social...\n")
from Social import social_run
print("Importações concluídas.\n\n")
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

def saude():
    logging.info("Começa saúde")
    # saude_run()
    logging.info("Terminou saúde")

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

def social():
    logging.info("Começa social")
    social_run()
    logging.info("Terminou social")

if __name__ == "__main__":
    input_func = input("Digite o nome ou número da função a ser executada: \n0. Saude \n1. Renda \n2. Habitacao \n3. Emprego \n4. Demografia \n5. Financas \n6. Educacao \n7. Seguranca \n8. Territorio \n9. Social \n10. Todas \nResposta: ").strip().lower()
    if input_func == "renda" or input_func == "1":
        renda()
    elif input_func == "saude" or input_func == "0":
        saude() 
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
    elif input_func == "social" or input_func == "9":
        social()
    elif input_func == "todas" or input_func == "10":
        renda()
        habitacao()
        emprego()
        demografia()
        financas()
        educacao()
        seguranca()
        territorio()
        # saude()
    else:
        logging.error("Função inválida. Por favor, tente novamente.")

# if __name__ == "__main__":
#     territorio()