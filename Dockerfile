# Must be in sync with ${dep.trino.version} in pom.xml
FROM trinodb/trino:374

ARG VERSION

RUN rm -rf /usr/lib/trino/plugin/{accumulo,atop,bigquery,blackhole,cassandra,clickhouse,druid,elasticsearch,example-http,geospatial,google-sheets,hive,http-event-listener,iceberg,kafka,kinesis,kudu,local-file,memsql,ml,mongodb,mysql,oracle,password-authenticators,phoenix,phoenix5,pinot,postgresql,prometheus,raptor-legacy,redis,redshift,resource-group-managers,session-property-managers,sqlserver,teradata-functions,thrift,tpcds,tpch} \
    rm -rf /etc/trino/catalog/{tpcds,tpch}.properties \
    && ls -la /usr/lib/trino/plugin

ADD trino-rest-github/target/trino-rest-github-$VERSION/ /usr/lib/trino/plugin/github/
ADD trino-rest-twitter/target/trino-rest-twitter-$VERSION/ /usr/lib/trino/plugin/twitter/
ADD trino-rest-slack/target/trino-rest-slack-$VERSION/ /usr/lib/trino/plugin/slack/
ADD catalog/ /etc/trino/catalog/disabled/
ADD docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["/usr/lib/trino/bin/run-trino"]
