# SocioDataBot

O **SocioDataBot** da Vale tem como objetivo automatizar a coleta e atualização de dados socioeconômicos provenientes de fontes públicas. O robô foi desenvolvido em Python.

<br>

## Objetivo

O principal objetivo do projeto é garantir que o banco de dados socioeconômico da Vale seja constantemente atualizado com dados confiáveis e relevantes.

<br>

## Executar

### Com docker
**Com o docker desktop no seu windows, execute os seguintes comandos em um terminal do diretório do projeto:**

- docker build -t data-bot .
- docker run -it data-bot

<br>

**Alternativamente, se quiser um live-reload do seu código local para o container, substitua o último comando pelo seguinte:**

- docker run -v "$(pwd):/app" -it data-bot

<br>

**Após isso você terá um terminal para interagir com a aplicação conteinerizada.**

Para executar o script main.py, digite no terminal docker o comando:

- python3 main.py

<br>


### Com ambiente virtual Python:

Após o ambiente estar ativo, basta iniciar o main.py.

- python main.py

## Tecnologias Utilizadas

O robô faz uso das seguintes bibliotecas para executar suas funções de forma automatizada:

- **Pandas**: Utilizada para manipulação e análise de grandes volumes de dados, permitindo operações rápidas e eficientes.
- **basedosdados**: Facilita o acesso a dados abertos de fontes públicas como o IBGE, Ipea e outras, integrando esses dados ao banco de dados da Vale.
- **PySuS**: Biblioteca que permite o manuseio de dados relacionados ao Sistema Único de Saúde (SUS) e outras fontes de dados do setor público.
- **Geopandas**: Usada para o tratamento e análise de dados geoespaciais, permitindo trabalhar com coordenadas geográficas e visualização de informações em mapas.
- **Selenium**: Ferramenta que automatiza a navegação e extração de dados de sites dinâmicos, permitindo acessar informações que exigem interação com páginas da web, como formulários ou conteúdo carregado dinamicamente.

