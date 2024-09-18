FROM python:3-alpine

WORKDIR /app

RUN pip install --upgrade pip

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY dltool.py ./

COPY dltool.sh ./

VOLUME /app/inputDats /app/outputFiles

CMD ["sh"]
