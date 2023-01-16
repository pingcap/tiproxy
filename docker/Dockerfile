FROM alpine:edge as builder

ADD https://raw.fastgit.org/njhallett/apk-fastest-mirror/main/apk-fastest-mirror.sh /
RUN sed -i 's/https/http/g' /apk-fastest-mirror.sh && sh /apk-fastest-mirror.sh -t 50 && apk add --no-cache --progress git make go
ARG VERSION
ARG COMMIT
ARG GOPROXY
ADD . /proxy
RUN export GOPROXY=${GOPROXY} && cd /proxy && go mod download -x
RUN export VERSION=${VERSION} && export COMMIT=${COMMIT} && export GOPROXY=${GOPROXY} && cd /proxy && make cmd

FROM alpine:latest

COPY --from=builder /proxy/bin/* /bin
COPY --from=builder /proxy/conf/* /etc/proxy

EXPOSE 3080
EXPOSE 3081
EXPOSE 6000

ENTRYPOINT ["/bin/tiproxy", "-conf", "/etc/proxy/proxy.toml"]
