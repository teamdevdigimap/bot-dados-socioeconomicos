# Use uma imagem base do Ubuntu
FROM ubuntu:20.04

# Evita interações durante a instalação de pacotes
ENV DEBIAN_FRONTEND=noninteractive

# Atualiza o apt e instala dependências básicas
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    sudo \
    gnupg2 \
    lsb-release \
    ca-certificates \
    libx11-xcb1 \
    libglu1-mesa \
    libxi6 \
    libgconf-2-4 \
    libnss3 \
    libxss1 \
    libappindicator3-1 \
    libasound2 \
    xdg-utils \
    fonts-liberation \
    libappindicator1 \
    libindicator7 \
    python3.8 \
    python3.8-distutils \
    libpq-dev \
    build-essential \
    python3.8-dev \
    libxml2-dev \
    nano \
    firefox



# Instalar pip para o Python 3.8
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python3.8 get-pip.py

# Instalar o Google Chrome (versão mais recente)
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt install -y ./google-chrome-stable_current_amd64.deb



# Copiar o arquivo requirements.txt para dentro do container
COPY requirements.txt /app/

# Listar o conteúdo de /app para verificar se o arquivo está lá
RUN ls -l /app/

RUN pip install  -r /app/requirements.txt

RUN pip install PySUS==0.6.0


# Definir um diretório de trabalho
WORKDIR /app

# Copiar o código Python para o container
COPY . /app


ENV PATH="/usr/local/bin/chromedriver:$PATH"
ENV CHROME_BIN=/usr/bin/google-chrome-stable