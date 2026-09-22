package se.afshin.yavari.clientapp.config;

import java.util.List;

public final class ConfigException extends RuntimeException {
    private final List<String> problems;

    public ConfigException(Problems problems) {
        super(problems.message());
        this.problems = problems.list();
    }

    public List<String> problems() { return problems; }
}
