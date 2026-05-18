package se.afshin.yavari.kroxy.auth;

import io.kroxylicious.proxy.authentication.Principal;

public record Group(String name) implements Principal {}
