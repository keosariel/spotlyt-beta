from alpine:latest 

RUN apk add --update \
    build-base \
    cmake \
    curl-dev \
    gcc \
    gettext \
    linux-headers \
    openssl \
    py-pip \
    python3-dev \
    util-linux-dev \
    zlib-dev && \
    python3 -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip

ADD https://oligarchy.co.uk/xapian/1.4.18/xapian-core-1.4.18.tar.xz /tmp/
WORKDIR /tmp/
RUN tar Jxf /tmp/xapian-core-1.4.18.tar.xz
WORKDIR /tmp/xapian-core-1.4.18
RUN ./configure && make && make install

WORKDIR /spotlyt

COPY . .

RUN pip3 install --no-cache pipenv

RUN pipenv install --system --deploy --ignore-pipfile

EXPOSE 8080

CMD ["python3", "run.py"]