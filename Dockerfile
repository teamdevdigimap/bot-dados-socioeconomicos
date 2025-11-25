# Use uma imagem base do Ubuntu
FROM ubuntu:24.04

# Evita interações durante a instalação de pacotes
ENV DEBIAN_FRONTEND=noninteractive

# Permite instalar pacotes globais com pip (PEP 668)
ENV PIP_BREAK_SYSTEM_PACKAGES=1

# Atualiza o apt e instala dependências básicas
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    sudo \
    gnupg2 \
    lsb-release \
    ca-certificates \
    software-properties-common \
    libx11-xcb1 \
    libglu1-mesa \
    libxi6 \
    libnss3 \
    libxss1 \
    libasound2t64 \
    libdbus-glib-1-2 \
    libgtk-3-0 \
    xdg-utils \
    fonts-liberation \
    libu2f-udev \
    libvulkan1 \
    libgeos-dev \
    python3 \
    python3-pip \
    python3-full \
    build-essential \
    python3-dev \
    libxml2-dev \
    libpq-dev \
    nano
    


# Configurar e Instalar Firefox (Versão .DEB - Não Snap)
# Adiciona o repositório oficial da Mozilla Team
RUN add-apt-repository -y ppa:mozillateam/ppa

# Configura a prioridade para evitar que o Ubuntu tente instalar o Snap
RUN echo 'Package: *\nPin: release o=LP-PPA-mozillateam\nPin-Priority: 1001' | tee /etc/apt/preferences.d/mozilla-firefox

# Instala o Firefox real
RUN apt-get update && apt-get install -y firefox

# Instalar GeckoDriver (Necessário para Selenium com Firefox)
# Baixa a versão v0.34.0 (compatível com Firefox moderno)
RUN wget https://github.com/mozilla/geckodriver/releases/download/v0.34.0/geckodriver-v0.34.0-linux64.tar.gz && \
    tar -xvzf geckodriver-v0.34.0-linux64.tar.gz && \
    rm geckodriver-v0.34.0-linux64.tar.gz && \
    mv geckodriver /usr/local/bin/ && \
    chmod +x /usr/local/bin/geckodriver

# Instalar o Google Chrome (versão estável)
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb

# Copiar o arquivo requirements.txt para dentro do container
COPY requirements.txt /app/

# Listar o conteúdo de /app para verificar se o arquivo está lá
RUN ls -l /app/

RUN pip install  -r /app/requirements.txt

RUN pip install PySUS>=1.0


# Definir um diretório de trabalho
WORKDIR /app

# Copiar o código Python para o container
COPY . /app

ENV GOOGLE_APPLICATION_CREDENTIALS="/app/credentials.json"
ENV PATH="/usr/local/bin:$PATH"
ENV FIREFOX_BIN=/usr/bin/firefox
ENV CHROME_BIN=/usr/bin/google-chrome-stable