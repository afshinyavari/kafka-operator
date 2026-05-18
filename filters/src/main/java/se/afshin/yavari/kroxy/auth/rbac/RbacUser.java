package se.afshin.yavari.kroxy.auth.rbac;

import java.util.List;

public class RbacUser {
    public String name;
    public List<String> topics = List.of();
    public List<String> operations = List.of();
}
