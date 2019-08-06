package dblock;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DbLockTest {

    private HikariDataSource ds;
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://localhost:3306/test");
        config.setUsername("root");
        config.setPassword("");

        ds = new HikariDataSource(config);
        jdbcTemplate = new JdbcTemplate(ds);
        clearLock();
    }

    private void clearLock() {
        jdbcTemplate.update("delete from dist_lock");
    }

    @AfterEach
    void tearDown() {
        ds.close();
    }

    @Test
    void runInLock() {
        AtomicBoolean runned = new AtomicBoolean(false);
        DbLock lock = new DbLock(ds);
        lock.runInLock("BATCH", Duration.ofMinutes(1), () -> {
            runned.set(true);
        });

        assertEquals(true, runned.get());
    }

    @Test
    void cantRun_When_Others_Owned_Lock() {
        DbLock lock1 = new DbLock(ds);
        DbLock lock2 = new DbLock(ds);

        AtomicReference<String> runned = new AtomicReference<>();
        lock1.runInLock("LOCK", Duration.ofSeconds(5), () -> {
            runned.set("LOCK1");
            sleep(2000);
        });

        lock2.runInLock("LOCK", Duration.ofSeconds(5), () -> {
            runned.set("LOCK2");
        });

        assertEquals("LOCK1", runned.get());
    }

    @Test
    void canRun_When_My_Owned_Lock() {
        DbLock lock1 = new DbLock(ds);

        AtomicReference<String> runned = new AtomicReference<>();
        lock1.runInLock("LOCK", Duration.ofSeconds(5), () -> {
            runned.set("LOCK1-1");
            sleep(2000);
        });

        assertEquals("LOCK1-1", runned.get());

        lock1.runInLock("LOCK", Duration.ofSeconds(5), () -> {
            runned.set("LOCK1-2");
        });

        assertEquals("LOCK1-2", runned.get());
    }


    @Test
    void canRun_When_Others_Owned_Lock_Expired() {
        DbLock lock1 = new DbLock(ds);
        DbLock lock2 = new DbLock(ds);

        AtomicReference<String> runned = new AtomicReference<>();
        lock1.runInLock("LOCK", Duration.ofSeconds(2), () -> {
            runned.set("LOCK1");
            sleep(1000);
        });

        sleep(2000);

        lock2.runInLock("LOCK", Duration.ofSeconds(5), () -> {
            runned.set("LOCK2");
        });

        assertEquals("LOCK2", runned.get());
    }

    @Test
    void concurrent() {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

        ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (int i = 0 ; i <= 40 ; i++) {
            int idx = i;
            executorService.submit(() -> {
                DbLock lock = new DbLock(ds);
                lock.runInLock("C-LOCK", Duration.ofSeconds(15), () -> {
                    map.put("LOCK" + idx, "");
                    sleep(300);
                });
            });
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
        assertEquals(1, map.size());
    }

    private void sleep(int sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
        }
    }
}
