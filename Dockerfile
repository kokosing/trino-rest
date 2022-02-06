# Must be in sync with ${dep.trino.version} in pom.xml
FROM trinodb/trino:370

ARG VERSION

RUN rm -rf /usr/lib/trino/plugin/{accumulo,atop,bigquery,blackhole,cassandra,clickhouse,druid,elasticsearch,example-http,geospatial,google-sheets,hive,http-event-listener,iceberg,kafka,kinesis,kudu,local-file,memsql,ml,mongodb,mysql,oracle,password-authenticators,phoenix,phoenix5,pinot,postgresql,prometheus,raptor-legacy,redis,redshift,resource-group-managers,session-property-managers,sqlserver,teradata-functions,thrift,tpcds,tpch} \
    && ls -la /usr/lib/trino/plugin

ADD trino-rest-github/target/trino-rest-github-$VERSION/ /usr/lib/trino/plugin/github/
ADD trino-rest-twitter/target/trino-rest-twitter-$VERSION/ /usr/lib/trino/plugin/twitter/
ADD trino-rest-slack/target/trino-rest-slack-$VERSION/ /usr/lib/trino/plugin/slack/
