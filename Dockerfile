FROM alpine:3.6

COPY bin/kube-insight-logserver-alpine /usr/local/bin/kube-insight-logserver-alpine

ENTRYPOINT [ "/usr/local/bin/kube-insight-logserver-alpine" ]
