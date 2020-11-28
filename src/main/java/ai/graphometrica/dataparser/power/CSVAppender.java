package ai.graphometrica.dataparser.power;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.swing.plaf.IconUIResource;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Slf4j
public class CSVAppender implements Closeable, AutoCloseable {

    private final String fileName;
    private final int rowPeriod = 5000; //every 1000 rows writes to file
    private final int timePeriod = 10000; //every 5000 milliseconds writes to file
    private final String delimiter = ";";
    private final List<String> fieldSet = new ArrayList<>();
    private volatile boolean isWriting = false;
    private ReentrantLock lock = new ReentrantLock();
    private ConcurrentLinkedQueue<Map<String,String>> queue = new ConcurrentLinkedQueue<>();
    private Thread timeoutThread;
    private volatile boolean closed = false;

    public CSVAppender(String fileName) {
        this.fileName = fileName;
        timeoutThread = new Thread(this::run);
        timeoutThread.start();
    }

    public void close() {
        log.info("Closing appender");
        closed = true;
        new Thread(this::write).start();
        timeoutThread.interrupt();
    }

    public void append(Map<String, String> data) {
        if(closed) {
            throw new IllegalStateException("Appender is closed");
        }
        if(fieldSet.isEmpty()) {
            synchronized (fieldSet) {
                if(fieldSet.isEmpty()) {
                    fieldSet.addAll(data.keySet().stream().sorted().collect(Collectors.toList()));
                    log.info("Init columns: [{}]", String.join(",", fieldSet));
                }
            }
        } else {
            if (fieldSet.size() != data.keySet().size() && !fieldSet.containsAll(data.keySet())) {
                log.warn("Column size or column count is incorrect: [{}], expected [{}]",
                        String.join(",", data.keySet()),
                        String.join(",", fieldSet));
                return;
            }
        }

        queue.add(data);

        if(queue.size() >= rowPeriod) {
            //log.info("Start writing by rowPeriod {} ms", rowPeriod);
            new Thread(this::write).start();
        }

    }

    @SuppressWarnings("BusyWait")
    private void run() {
        while(true) {
            try {
                Thread.sleep(timePeriod);
                log.info("Start writing by timeout {} ms", timePeriod);
                write();
                if(closed) {
                    log.info("Exit timeout thread");
                    break;
                }
            } catch (InterruptedException e) {
                log.info("Interrupted timeout thread");
                break;
            }

        }
    }

    private void write() {
        if (lock.tryLock()) {
            try {
                Map<String, String> data;
                String formatted = null;
                try (Writer fos = new FileWriter(fileName, true)) {
                    int count = 0;
                    while ((data = queue.poll()) != null) {
                        count++;
                        formatted = String.join(delimiter, data.entrySet()
                                .stream()
                                .sorted(Map.Entry.comparingByKey())
                                .map(Map.Entry::getValue)
                                .collect(Collectors.joining(delimiter)));
                        fos.append(formatted).append("\n");
                    }
                    log.info("Rows append: {}", count);
                    log.info("LastRow is {}", formatted);
                } catch (IOException e) {
                    log.error("Cant write to file", e);
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
