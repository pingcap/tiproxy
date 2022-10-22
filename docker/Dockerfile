FROM alpine:edge as builder

ADD https://raw.githubusercontent.com/njhallett/apk-fastest-mirror/c4ca44caef3385d830fea34df2dbc2ba4a17e021/apk-fastest-mirror.sh /
RUN sh /apk-fastest-mirror.sh -t 50 && apk add --no-cache --progress git make go
ARG BUILDFLAGS
ARG GOPROXY
ADD . /proxy
RUN export BUILDFLAGS=${BUILDFLAGS} && export GOPROXY=${GOPROXY} && cd /proxy && make cmd

FROM alpine:latest

COPY --from=builder /proxy/bin/* /bin
COPY --from=builder /proxy/conf/* /etc/proxy

EXPOSE 3080
EXPOSE 3081
EXPOSE 6000

ENTRYPOINT ["/bin/tiproxy", "-conf", "/etc/proxy/proxy.yaml"]
