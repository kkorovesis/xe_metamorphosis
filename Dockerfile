FROM python:3

RUN mkdir /app

RUN apt-get install default-libmysqlclient-dev
ADD requirements.txt /app/requirements.txt
RUN pip install -r  /app/requirements.txt

COPY ./execute.sh /app/
RUN chmod +x /app/execute.sh

ADD xe_consume.py /app/xe_consume.py
ADD unit_tests.py /app/unit_tests.py
ADD utils.py /app/utils.py
ADD /.log /app/.log

WORKDIR /app
