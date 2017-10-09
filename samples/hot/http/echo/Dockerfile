FROM python:3.6.2


RUN mkdir /code
ADD . /code/
WORKDIR /code/
ARG http_proxy=http://www-proxy-lon.uk.oracle.com:80/
ARG https_proxy=http://www-proxy-lon.uk.oracle.com:80/
ARG no_proxy=localhost,127.0.0.1,docker.for.mac.localhost
RUN pip install -i http://docker.for.mac.localhost:3141/testuser/dev/+simple/ --trusted-host docker.for.mac.localhost --pre -r requirements.txt
WORKDIR /code/

ENTRYPOINT ["python3", "func.py"]
