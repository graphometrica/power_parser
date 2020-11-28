package ai.graphometrica.dataparser.power;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

//@SpringBootApplication
@Slf4j
public class PowerApplication {

    public static AtomicLong along = new AtomicLong(0);

    public static class ObjectPair {
        public String SubjectId;
        public String PowerSystemId;

        public ObjectPair(String subjectId, String powerSystemId) {
            SubjectId = subjectId.replace("\"", "");
            PowerSystemId = powerSystemId.replace("\"", "");
        }
    }

    public static class DataRequest {
        LocalDate date;
        int hour;
        ObjectPair pair;
    }

    public static Map<String, String> getQueryMap(String query) {
        String[] params = query.split("&");
        Map<String, String> map = new HashMap<String, String>();

        for (String param : params) {
            String name = param.split("=")[0];
            String value = param.split("=")[1];
            map.put(name, value);
        }
        return map;
    }

    @Slf4j
    public static class DataRequestExecutor implements Runnable {

        private final String url = "http://br.so-ups.ru/webapi/api/map/MapPartial?MapType=0&Date=%s&Hour=%d&PowerSystemId=%s&SubjectId=%s";
        private final String finalUrl;
        private final List<String> badList;
        private final CSVAppender appender;

        public DataRequestExecutor(DataRequest request, CSVAppender appender, List<String> badList) {
            finalUrl = String.format(url, request.date.toString(), request.hour, request.pair.PowerSystemId, request.pair.SubjectId);
            this.badList = badList;
            this.appender = appender;
        }

        public DataRequestExecutor(String finalUrl, CSVAppender appender, List<String> badList) {
            this.finalUrl = finalUrl;
            this.badList = badList;
            this.appender = appender;
        }

        @Override
        public void run() {
            try {
                HashMap<String, String> row = new HashMap<>();
                var url = new URL(finalUrl);
                var data = new ObjectMapper().readTree(url);
                var localtime = LocalDateTime.of(
                        LocalDate.parse(getQueryMap(url.getQuery()).get("Date")),
                        LocalTime.of(Integer.parseInt(getQueryMap(url.getQuery()).get("Hour")), 0));
                row.put("1Date", localtime.toString());
                row.put("3PowerSystemId", getQueryMap(url.getQuery()).get("PowerSystemId"));
                row.put("2SubjectId", getQueryMap(url.getQuery()).get("SubjectId"));
                row.put("4IBR_ActualConsumption", data.get("MainArea").get("IBR_ActualConsumption").asText().replaceAll("\\D", ""));
                row.put("5IBR_ActualGeneration", data.get("MainArea").get("IBR_ActualGeneration").asText().replaceAll("\\D", ""));
                row.put("6IBR_AveragePrice", data.get("MainArea").get("IBR_AveragePrice").asText().replaceAll("\\D", ""));
                row.put("7IBR_PlannedConsumption", data.get("MainArea").get("IBR_ActualConsumption").asText().replaceAll("\\D", ""));
                row.put("8IBR_PlannedGeneration", data.get("MainArea").get("IBR_PlannedConsumption").asText().replaceAll("\\D", ""));
                row.put("URL", finalUrl);
                appender.append(row);
                along.decrementAndGet();
            } catch (Exception e) {
                badList.add(url);
                log.warn("Error parse url: {}", e.getMessage());
            }
        }
    }

    public PowerApplication() throws InterruptedException, IOException {

        List<String> badList = new CopyOnWriteArrayList<>();

        String url = "http://br.so-ups.ru/webapi/api/map/MapPartial?MapType=0&Date=%s&Hour=%d&PowerSystemId=%s&SubjectId=%s";
        ObjectMapper mapper = new ObjectMapper();

        List<ObjectPair> objects = new ArrayList<>();

        var result = mapper.readTree(new File("D:\\Downloads\\Telegram Desktop\\energo_subjects.json"));
        result.elements().forEachRemaining(node -> {
            objects.add(new ObjectPair(
                            node.get("SubjectId").asText(),
                            node.get("PowerSystemId").asText()
                    )
            );
        });

        LocalDate workDate = LocalDate.now().minusMonths(3).plusDays(1);
        LocalDate to = LocalDate.now().minusYears(3);

        ExecutorService pool = Executors.newFixedThreadPool(10);

        try (CSVAppender adapter = new CSVAppender("output2.csv")) {

            while ((workDate = workDate.minusDays(1)).isAfter(to)) {
                var date = workDate;
                objects.forEach(o -> {
                    for (int hour = 0; hour < 24; hour++) {
                        along.incrementAndGet();
                        pool.execute(new DataRequestExecutor(String.format(url, date.toString(), hour, o.PowerSystemId, o.SubjectId), adapter, badList));
                    }
                });
            }
            pool.shutdown();
            pool.awaitTermination(1, TimeUnit.DAYS);
        }

        ExecutorService pool2 = Executors.newFixedThreadPool(5);
        List<String> finalBadList = new CopyOnWriteArrayList<>();

        try (CSVAppender adapter = new CSVAppender("output2.csv")) {
            badList.forEach(badurl -> {
                pool2.execute(new DataRequestExecutor(badurl, adapter, finalBadList));
            });
            pool.shutdown();
            pool.awaitTermination(1, TimeUnit.DAYS);
        }
        for(String badurl : finalBadList) {
            System.err.println(badurl);
        }
    }


    public static void main(String[] args) throws InterruptedException, IOException {
        new PowerApplication();
    }

}
