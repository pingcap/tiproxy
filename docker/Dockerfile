FROM alpine:latest

EXPOSE 3080
EXPOSE 3081
EXPOSE 6000

ADD cmd proxy/cmd
ADD lib proxy/lib
ADD pkg proxy/pkg
ADD conf proxy/conf
ADD go.* docker/apk-fastest-mirror.sh Makefile proxy/
RUN sh ./proxy/apk-fastest-mirror.sh
RUN apk add --no-cache --progress git make go
ARG BUILDFLAGS
ARG GOPROXY
RUN export BUILDFLAGS=${BUILDFLAGS} && export GOPROXY=${GOPROXY} && cd proxy && make cmd && cp bin/* /bin/ && cp -a conf /etc/proxy && cd .. && rm -rf proxy
RUN rm -rf $(go env GOMODCACHE GOCACHE) && apk del git make go
ENTRYPOINT ["/bin/tiproxy", "-conf", "/etc/proxy/proxy.yaml"]
