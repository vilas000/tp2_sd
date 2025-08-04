# Imagem base Python leve
FROM python:3.9-slim-buster

# Diretório de trabalho dentro do contêiner
WORKDIR /app

# Copiar o script do cliente para o diretório de trabalho
COPY client.py .

# Executar o script do cliente
CMD ["python", "client.py"]