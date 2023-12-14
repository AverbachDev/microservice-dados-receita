FROM golang:1.21.5-bullseye

WORKDIR /go/src
ENV PATH="/go/bin:${PATH}"
ENV TZ="America/Sao_Paulo"

RUN apt-get update && \
    apt-get install build-essential librdkafka-dev -y

COPY . .

RUN chmod 777 ./download.sh

RUN GOOS=linux go build -ldflags="-s -w" -o dados-receita
ENTRYPOINT ["./dados-receita"]
