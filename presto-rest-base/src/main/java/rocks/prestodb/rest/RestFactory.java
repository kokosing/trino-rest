package rocks.prestodb.rest;

import java.util.Map;

public interface RestFactory
{
    Rest create(Map<String, String> config);
}
