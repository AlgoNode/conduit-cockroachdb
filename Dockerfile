FROM golang:1.21 as build-env

RUN groupadd -g 4160 conduit && useradd -u 4160 -g 4160 conduit

WORKDIR /go/src/app
COPY . /go/src/app

RUN make && strip cmd/conduit/conduit

FROM scratch

COPY --from=build-env /go/src/app/cmd/conduit/conduit /app/conduit
COPY --from=build-env /etc/passwd /etc/passwd
USER conduit

CMD ["/app/conduit","-d","/data"]
