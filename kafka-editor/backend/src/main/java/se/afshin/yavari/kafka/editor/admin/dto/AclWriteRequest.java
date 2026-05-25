package se.afshin.yavari.kafka.editor.admin.dto;

import se.afshin.yavari.kafka.editor.api.ConnectionConfig;

/** Body of POST / DELETE /api/admin/acls. */
public record AclWriteRequest(ConnectionConfig connection, AclEntry acl) {
}
