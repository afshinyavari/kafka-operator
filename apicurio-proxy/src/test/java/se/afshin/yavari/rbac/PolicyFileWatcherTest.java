package se.afshin.yavari.rbac;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PolicyFileWatcherTest {

    @TempDir
    Path dir;

    private final List<String> loaded = new CopyOnWriteArrayList<>();
    private PolicyFileWatcher watcher;

    @AfterEach
    void tearDown() {
        if (watcher != null) watcher.stop();
    }

    /** Loader that records what it was given and rejects content starting with "bad". */
    private void record(byte[] content) {
        String text = new String(content, StandardCharsets.UTF_8);
        if (text.startsWith("bad")) throw new IllegalArgumentException("unparseable: " + text);
        loaded.add(text);
    }

    private PolicyFileWatcher watch(Path file) {
        watcher = new PolicyFileWatcher(file, this::record);
        return watcher;
    }

    /**
     * Lays the volume out the way kubelet does for a ConfigMap: the visible file is a symlink
     * through {@code ..data}, which points at a timestamped directory. Returns the visible file.
     */
    private Path configMapVolume(String content) throws Exception {
        publish("..rev1", content);
        Files.createSymbolicLink(dir.resolve("..data"), Path.of("..rev1"));
        return Files.createSymbolicLink(dir.resolve("policy.yaml"), Path.of("..data", "policy.yaml"));
    }

    private void publish(String revision, String content) throws Exception {
        Path revisionDir = Files.createDirectory(dir.resolve(revision));
        Files.writeString(revisionDir.resolve("policy.yaml"), content);
    }

    /** A ConfigMap update: new timestamped directory, then {@code ..data} is swapped atomically.
     *  The visible {@code policy.yaml} symlink itself is never touched. */
    private void updateConfigMap(String revision, String content) throws Exception {
        publish(revision, content);
        Path tmp = Files.createSymbolicLink(dir.resolve("..data_tmp"), Path.of(revision));
        Files.move(tmp, dir.resolve("..data"), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    @Test
    void loadsNewContentAfterConfigMapSymlinkSwap() throws Exception {
        PolicyFileWatcher w = watch(configMapVolume("v1"));
        w.loadInitial();

        updateConfigMap("..rev2", "v2");

        assertThat(w.poll()).isTrue();
        assertThat(loaded).containsExactly("v1", "v2");
    }

    @Test
    void unchangedContentIsNotLoadedAgain() throws Exception {
        Path file = Files.writeString(dir.resolve("policy.yaml"), "v1");
        PolicyFileWatcher w = watch(file);
        w.loadInitial();

        assertThat(w.poll()).isFalse();
        assertThat(loaded).containsExactly("v1");
    }

    @Test
    void initialLoadFailurePropagates() throws Exception {
        Path file = Files.writeString(dir.resolve("policy.yaml"), "bad");

        assertThatThrownBy(() -> watch(file).loadInitial())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectedContentIsNotRetriedUntilItChanges() throws Exception {
        Path file = Files.writeString(dir.resolve("policy.yaml"), "v1");
        List<String> attempts = new CopyOnWriteArrayList<>();
        watcher = new PolicyFileWatcher(file, content -> {
            attempts.add(new String(content, StandardCharsets.UTF_8));
            record(content);
        });
        watcher.loadInitial();

        Files.writeString(file, "bad");
        assertThat(watcher.poll()).isFalse();
        assertThat(watcher.poll()).isFalse();
        assertThat(attempts).containsExactly("v1", "bad");

        Files.writeString(file, "v2");
        assertThat(watcher.poll()).isTrue();
        assertThat(loaded).containsExactly("v1", "v2");
    }

    @Test
    void unreadableFileIsSkippedAndPickedUpWhenItReturns() throws Exception {
        Path file = Files.writeString(dir.resolve("policy.yaml"), "v1");
        PolicyFileWatcher w = watch(file);
        w.loadInitial();

        Files.delete(file);
        assertThat(w.poll()).isFalse();

        Files.writeString(file, "v2");
        assertThat(w.poll()).isTrue();
        assertThat(loaded).containsExactly("v1", "v2");
    }

    @Test
    void startPollsInTheBackground() throws Exception {
        Path file = configMapVolume("v1");
        CountDownLatch sawV2 = new CountDownLatch(1);
        watcher = new PolicyFileWatcher(file, content -> {
            record(content);
            if (new String(content, StandardCharsets.UTF_8).equals("v2")) sawV2.countDown();
        });
        watcher.loadInitial();
        watcher.start(Duration.ofMillis(20));

        updateConfigMap("..rev2", "v2");

        assertThat(sawV2.await(5, TimeUnit.SECONDS)).isTrue();
    }
}
