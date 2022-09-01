FROM alpine:latest

EXPOSE 3080
EXPOSE 3081
EXPOSE 6000

ADD ./ proxy
RUN sh ./proxy/docker/apk-fastest-mirror.sh
RUN apk add git make go
RUN cd proxy && make build && cp bin/* /bin/ && cp -a conf /etc/proxy && cd .. && rm -rf proxy
RUN apk del git make go
