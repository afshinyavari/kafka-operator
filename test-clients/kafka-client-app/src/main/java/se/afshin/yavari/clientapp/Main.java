package se.afshin.yavari.clientapp;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;

/** Replaced with the real startup sequence in Task 10. */
@QuarkusMain
public class Main implements QuarkusApplication {
    @Override
    public int run(String... args) {
        Quarkus.waitForExit();
        return 0;
    }
}
