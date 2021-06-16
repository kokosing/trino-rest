FROM trinodb/trino:358

ARG VERSION

RUN rm -rf /usr/lib/trino/plugin/{accumulo,bigquery,cassandra,druid,example-http,google-sheets,iceberg,kafka,kudu,ml,mysql,password-authenticators,phoenix5,postgresql,raptor-legacy,redshift,session-property-managers,teradata-functions,tpcds,atop,blackhole,clickhouse,elasticsearch,geospatial,hive-hadoop2,kinesis,local-file,memsql,mongodb,oracle,phoenix,pinot,prometheus,redis,resource-group-managers,sqlserver,thrift,tpch}

ADD trino-rest-github/target/trino-rest-github-$VERSION/ /usr/lib/trino/plugin/github/
ADD trino-rest-twitter/target/trino-rest-twitter-$VERSION/ /usr/lib/trino/plugin/twitter/
ADD trino-rest-slack/target/trino-rest-slack-$VERSION/ /usr/lib/trino/plugin/slack/
