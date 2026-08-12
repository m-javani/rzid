FROM ubuntu:24.04

RUN apt-get update && apt-get install -y ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Binary is copied to root by CI
COPY rzid /opt/rzid/rzid

RUN chmod +x /opt/rzid/rzid

EXPOSE 8080

CMD ["/opt/rzid/rzid"]