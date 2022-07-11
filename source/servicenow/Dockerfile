FROM google/cloud-sdk:slim
RUN ["mkdir", "/app"]
WORKDIR /app
COPY ./requirements.txt .
RUN ["pip3", "install", "--upgrade", "pip"]
RUN ["pip3", "install", "-r", "requirements.txt"]
COPY ./main.py .
COPY ./modules.py .
COPY ./init.py .
ENTRYPOINT ["python3", "main.py"]
EXPOSE 8080