FROM rust:1.52.1 as build
WORKDIR /polixy
ADD . /polixy
RUN cargo build -p polixy-client --release

FROM gcr.io/distroless/cc:nonroot
COPY --from=build /polixy/target/release/polixy-client /
ENTRYPOINT ["/polixy-client"]
