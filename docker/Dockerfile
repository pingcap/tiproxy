FROM alpine:edge as builder

RUN apk add --no-cache --progress git make go
ARG VERSION
ARG BRANCH
ARG COMMIT
ARG GOPROXY
ADD . /proxy
RUN export GOPROXY=${GOPROXY} && cd /proxy && go mod download -x
RUN export VERSION=${VERSION} && export BRANCH=${BRANCH} && export COMMIT=${COMMIT} && export GOPROXY=${GOPROXY} && cd /proxy && make cmd

FROM alpine:latest

COPY --from=builder /proxy/bin/* /bin/
COPY --from=builder /proxy/conf/* /etc/proxy/

EXPOSE 3080
EXPOSE 3081
EXPOSE 6000

ENTRYPOINT ["/bin/tiproxy", "--config", "/etc/proxy/proxy.toml"]
