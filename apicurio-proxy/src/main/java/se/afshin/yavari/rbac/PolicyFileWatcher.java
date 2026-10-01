package se.afshin.yavari.rbac;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;

/**
 * Polls the policy file and hands its content to the loader whenever the content changes.
 *
 * <p>Polling by content, rather than a {@code WatchService} on the file name, is what makes a
 * Kubernetes ConfigMap update visible: kubelet never writes to the mounted file, it swaps the
 * {@code ..data} symlink the file resolves through. A load that fails keeps whatever the loader
 * had before; the rejected content is not offered again until the file changes.
 */
final class PolicyFileWatcher {

    @FunctionalInterface
    interface Loader {
        void load(byte[] content) throws Exception;
    }

    private final Path file;
    private final Loader loader;
    private byte[] lastSeen;
    private boolean unreadable;
    private volatile boolean running = true;
    private Thread thread;

    PolicyFileWatcher(Path file, Loader loader) {
        this.file = file;
        this.loader = loader;
    }

    /** First load. A failure propagates, so a broken policy file fails startup. */
    void loadInitial() throws Exception {
        byte[] content = Files.readAllBytes(file);
        loader.load(content);
        lastSeen = content;
    }

    /** One poll; {@code true} when changed content was loaded. */
    boolean poll() {
        byte[] content;
        try {
            content = Files.readAllBytes(file);
        } catch (IOException e) {
            if (!unreadable) System.err.println("[PolicyFileWatcher] Cannot read " + file + ", keeping current policy: " + e);
            unreadable = true;
            return false;
        }
        unreadable = false;
        if (Arrays.equals(content, lastSeen)) return false;
        lastSeen = content;
        try {
            loader.load(content);
            return true;
        } catch (Exception e) {
            System.err.println("[PolicyFileWatcher] Reload of " + file + " failed, keeping current policy: " + e.getMessage());
            return false;
        }
    }

    void start(Duration interval) {
        thread = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(interval.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                poll();
            }
        }, "policy-watcher");
        thread.setDaemon(true);
        thread.start();
    }

    void stop() {
        running = false;
        if (thread != null) thread.interrupt();
    }
}
