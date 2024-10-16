FROM alpine:edge as builder

RUN apt-get -y -q update && \
     apt-get -y -q install software-properties-common && \
     apt-get install -qqy \
         dos2unix \
         default-mysql-client \
         psmisc \
         vim
ARG VERSION
ARG BRANCH
ARG COMMIT
ARG GOPROXY
ADD . /proxy
RUN export GOPROXY=${GOPROXY} && cd /proxy && go mod download -x
RUN export VERSION=${VERSION} && export BRANCH=${BRANCH} && export COMMIT=${COMMIT} && export GOPROXY=${GOPROXY} && cd /proxy && make cmd

FROM golang:1.22-bullseye

COPY --from=builder /proxy/bin/* /bin/
COPY --from=builder /proxy/conf/* /etc/proxy/

EXPOSE 3080
EXPOSE 3081
EXPOSE 6000

ENTRYPOINT ["/bin/tiproxy", "-conf", "/etc/proxy/proxy.toml"]
