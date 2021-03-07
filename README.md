trino-rest
==========

This is a [Trino](http://trino.io/) connector to access RESTful APIs. Please keep in mind that this is not production ready and it was created for tests.

# Usage

Copy jar files in the target directory to the plugins directory on every node in your Trino cluster.
Create a `rest.properties` file in your Trino catalog directory and point to a remote repo.
You can also use a path to a local repo if it's available on every worker node.

```
connector.name=github
user=trinodb
repo=trino
token=${ENV:GITHUB_TOKEN}
```

After reloading Trino, you should be able to connect to the `github` catalog and see the following tables in the `default` schema:
* `issues`

# Build

Run all the unit test classes.
```
mvn test
```

Creates a deployable jar file
```
mvn clean compile package
```

Copy jar files in target directory to use git connector in your Trino cluster.
```
cp -p trino-rest-github/target/*.jar ${PLUGIN_DIRECTORY}/github/
```

# Deploy

An example command to run the Trino server with the git plugin and catalog enabled:

```bash
src=$(git rev-parse --show-toplevel)
docker run \
  -v $src/target/trino-git-0.3-SNAPSHOT:/usr/lib/trino/plugin/git \
  -v $src/catalog:/usr/lib/trino/default/etc/catalog \
  -p 8080:8080 \
  --name trino \
  -d \
  trinodb/trino:353
```

Connect to that server using:
```bash
docker run -it --rm --link trino trinodb/trino:353 trino --server trino:8080 --catalog github --schema default
```