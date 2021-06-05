package pl.net.was.rest.github;

import io.trino.spi.connector.SchemaTableName;
import pl.net.was.rest.RestTableHandle;

public enum GithubTable
{
    ORGS("orgs"),
    USERS("users"),
    REPOS("repos"),
    PULLS("pulls"),
    PULL_COMMITS("pull_commits"),
    REVIEWS("reviews"),
    REVIEW_COMMENTS("review_comments"),
    ISSUES("issues"),
    ISSUE_COMMENTS("issue_comments"),
    RUNS("runs"),
    JOBS("jobs"),
    STEPS("steps"),
    ARTIFACTS("artifacts");

    private final String name;

    GithubTable(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    public static GithubTable valueOf(RestTableHandle table)
    {
        return valueOf(table.getSchemaTableName());
    }

    public static GithubTable valueOf(SchemaTableName schemaTable)
    {
        return valueOf(schemaTable.getTableName().toUpperCase());
    }
}
