# Builds the rendezvous-relay server. See README.md.
FROM swift:6.2-noble AS build
WORKDIR /src
COPY Package.swift ./
COPY Package.resolved* ./
RUN swift package resolve
COPY Sources ./Sources
COPY Tests ./Tests
RUN swift build -c release --product rendezvous-relay --static-swift-stdlib \
    && cp "$(swift build -c release --show-bin-path)/rendezvous-relay" /rendezvous-relay

FROM ubuntu:noble
RUN useradd --system --no-create-home relay
COPY --from=build /rendezvous-relay /usr/local/bin/rendezvous-relay
USER relay
EXPOSE 3340
ENTRYPOINT ["/usr/local/bin/rendezvous-relay"]
