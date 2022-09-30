FROM alpine:latest

EXPOSE 3080
EXPOSE 3081
EXPOSE 6000

ADD . https://raw.githubusercontent.com/njhallett/apk-fastest-mirror/main/apk-fastest-mirror.sh /proxy
RUN sh ./proxy/apk-fastest-mirror.sh -t 50 && apk add --no-cache --progress git make go
ARG BUILDFLAGS
ARG GOPROXY
RUN export BUILDFLAGS=${BUILDFLAGS} && export GOPROXY=${GOPROXY} && cd proxy && make cmd && cp bin/* /bin/ && cp -a conf /etc/proxy && cd .. && rm -rf proxy && rm -rf $(go env GOMODCACHE GOCACHE GOPATH) && apk del git make go
ENTRYPOINT ["/bin/tiproxy", "-conf", "/etc/proxy/proxy.yaml"]
