################################################
# Build container to assemble the jar
FROM gradle:6.3.0-jdk11 AS builder
ARG DEBIAN_FRONTEND=noninteractive
USER root
RUN apt-get update \
  	&& apt-get clean \
  	&& apt-get install -y -qq --no-install-recommends apt-utils maven \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . /app
RUN gradle clean build --stacktrace

################################################
# Final container only containing jar and data
FROM ubuntu:20.04 AS runner
ARG DEBIAN_FRONTEND=noninteractive
RUN    apt-get update \
  	&& apt-get clean \
  	&& apt-get install -y -qq --no-install-recommends \
			apt-utils \
			openjdk-11-jre \
    && rm -rf /tmp/* /var/lib/apt/lists/* /var/tmp/* /usr/share/icons \
    && apt-get autoclean \
    && apt-get clean \
    && apt-get autoremove
# Add user to not run as root
RUN mkdir -p /app
RUN groupadd -g 999 appuser && useradd -r -u 999 -g appuser appuser
RUN chown -v -R appuser:appuser /app
# NOTE: removing version number in jar file due to wildcard problems in ENTRYPOINT (works with CMD)
COPY --from=builder /app/build/libs/techmap-scraping-system-*.jar /app/techmap-scraping-system.jar
# COPY --from=builder /app/build/libs/scrape.jar /app/techmap-scraping-system.jar
COPY --from=builder /app/start.sh /app/
RUN chmod +rx /app/start.sh
USER appuser
WORKDIR /app
ENV DOCKER_TAG=$DOCKER_TAG PATH=/app:$PATH
ENTRYPOINT ["./start.sh"]
