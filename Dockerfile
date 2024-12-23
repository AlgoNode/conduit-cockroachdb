FROM golang:1.23 AS build-env

RUN groupadd -g 4160 conduit && useradd -u 4160 -g 4160 conduit
#RUN git clone https://github.com/AlgoNode/conduit-cockroachdb.git
#RUN mv conduit-cockroachdb /go/src/app
COPY . /go/src/app
WORKDIR /go/src/app
RUN go mod tidy
RUN make && strip cmd/conduit/conduit

FROM gcr.io/distroless/static-debian12

COPY --from=build-env /go/src/app/cmd/conduit/conduit /app/conduit
COPY --from=build-env /etc/passwd /etc/passwd
USER conduit

CMD ["/app/conduit","-d","/data"]
