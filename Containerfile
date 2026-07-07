ARG PGVER=18
FROM postgres:${PGVER}

LABEL org.opencontainers.image.authors="The DBT-5 Authors"
LABEL org.opencontainers.image.description="Database Test 5 (DBT-5): Fair Use TPC Benchmark(TM) E; THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC."
LABEL org.opencontainers.image.documentation="https://osdldbt.github.io/dbt5/"
LABEL org.opencontainers.image.licenses="Artistic-2.0"
LABEL org.opencontainers.image.source="https://github.com/osdldbt/dbt5"
LABEL org.opencontainers.image.title="dbt5"
LABEL org.opencontainers.image.url="https://github.com/osdldbt/dbt5"

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=en_US.UTF-8

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential \
            ca-certificates cmake git postgresql-server-dev-${PG_MAJOR} && \
    rm -rf /var/lib/apt/lists/* && \
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias \
            en_US.UTF-8

COPY . /usr/local/src/dbt5
WORKDIR /usr/local/src/dbt5
RUN cmake -H. -B/tmp/build -DCMAKE_INSTALL_PREFIX=/usr && \
    cmake --build /tmp/build && \
    cmake --install /tmp/build && \
    cp -a /tmp/build/_deps/egen-src /opt/egen && \
    dbt5-build-egen --include-dir=src/include --patch-dir=patches \
        --source-dir=src /opt/egen && \
    make -C storedproc/pgsql/c USE_PGXS=1 install && \
    rm -rf /tmp/build
