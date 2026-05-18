package se.afshin.yavari.kroxy.auth.mtls;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.authentication.User;

import javax.security.auth.x500.X500Principal;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class CnSubjectBuilder implements TransportSubjectBuilder {

    private static final Pattern CN_PATTERN = Pattern.compile("(?:^|,)\\s*CN=([^,]+)", Pattern.CASE_INSENSITIVE);

    @Override
    public CompletionStage<Subject> buildTransportSubject(Context context) {
        Subject subject = context.clientTlsContext()
                .flatMap(tls -> tls.clientCertificate())
                .map(cert -> extractCn(cert))
                .filter(cn -> !cn.isEmpty())
                .map(cn -> new Subject(new User(cn)))
                .orElse(Subject.anonymous());
        return CompletableFuture.completedFuture(subject);
    }

    private static String extractCn(X509Certificate cert) {
        String dn = cert.getSubjectX500Principal().getName(X500Principal.RFC2253);
        Matcher m = CN_PATTERN.matcher(dn);
        return m.find() ? m.group(1).trim() : "";
    }
}
