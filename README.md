trino-rest
==========

This is a [Trino](http://trino.io/) connector to access RESTful APIs. Please keep in mind that this is not production ready and it was created for tests.

# Quick Start

To run a Docker container with one of the connectors, set the `GITHUB_TOKEN` environmental variable, and run the following:
```bash
docker run \
  --tmpfs /etc/trino/catalog \
  -v $(pwd)/catalog/github.properties:/etc/trino/catalog/github.properties \
  -e GITHUB_TOKEN \
  -p 8080:8080 \
  --name trino-rest-github \
  nineinchnick/trino-rest:0.3
```

Then use your favourite SQL client to connect to Trino running at http://localhost:8080

# Usage

Download one of the ZIP packages, unzip it and copy the `trino-rest-github-0.3` directory to the plugin directory on every node in your Trino cluster.
Create a `github.properties` file in your Trino catalog directory and point to a remote repo.
You can also use a path to a local repo if it's available on every worker node.

```
connector.name=github
token=${ENV:GITHUB_TOKEN}
```

After reloading Trino, you should be able to connect to the `github` catalog and see the following tables in the `default` schema:
* `orgs`
* `users`
* `repos`
* `issues`
* `issue_comments`
* `pulls`
* `pull_commits`
* `reviews`
* `review_comments`
* `runs`
* `jobs`
* `steps`
* `artifacts`
* `runners'

# Development

For more information, see the README files in connector directories:
* [trino-rest-github](trino-rest-github/README.md)
