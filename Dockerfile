ARG TRINO_VERSION
FROM trinodb/trino:$TRINO_VERSION

USER root
RUN set -xeu && \
    apt-get update && \
    apt-get install --yes jq && \
    rm -rf /var/lib/apt/lists/*
USER trino:trino

ARG VERSION

RUN rm -rf /usr/lib/trino/plugin/{accumulo,atop,bigquery,blackhole,cassandra,clickhouse,delta-lake,druid,elasticsearch,example-http,geospatial,google-sheets,hive,http-event-listener,iceberg,kafka,kinesis,kudu} \
    && rm -rf /usr/lib/trino/plugin/{local-file,memsql,ml,mongodb,mysql,oracle,password-authenticatorsphoenix5,pinot,postgresql,prometheus,raptor-legacy,redis,redshift,resource-group-managers,session-property-managers} \
    && rm -rf /usr/lib/trino/plugin/{sqlserver,teradata-functions,thrift,tpcds,tpch} \
    && rm -rf /etc/trino/catalog/{tpcds,tpch}.properties \
    && ls -la /usr/lib/trino/plugin

ADD trino-rest-github/target/trino-rest-github-$VERSION/ /usr/lib/trino/plugin/github/
ADD trino-rest-twitter/target/trino-rest-twitter-$VERSION/ /usr/lib/trino/plugin/twitter/
ADD trino-rest-slack/target/trino-rest-slack-$VERSION/ /usr/lib/trino/plugin/slack/
ADD catalog/ /etc/trino/catalog/disabled/
ADD docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["/usr/lib/trino/bin/run-trino"]
