package ai.graphometrica.dataparser.power;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class PostgresImportPSID {

    public static void main(String[] args) throws Exception {

        String connectionURL = "jdbc:postgresql://35.226.152.97:5432/minenergo";
        Class.forName("org.postgresql.Driver");
        Connection con = DriverManager.getConnection(connectionURL, "*", "*");

        File file = new File("d:\\work\\AHA\\Генерация и потребление_01.01.2017.csv");    //creates a new file instance
        FileReader fr = new FileReader(file);   //reads the file
        BufferedReader br = new BufferedReader(fr);  //creates a buffering character input stream
        StringBuffer sb = new StringBuffer();    //constructs a string buffer with no characters
        String line;
        int batchCount = 0;
        int batchSize = 10000;

        String sql = "insert into minenergo.power_sys(ds, power_sys_id, use_fact, use_plan, gen_fact, get_plan) VALUES (?,?,?,?,?,?)";

        PreparedStatement stmt = con.prepareStatement(sql);

        var dtf = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm");
        while ((line = br.readLine()) != null) {
            try {

                var values = line.split(";");
                batchCount++;
                stmt.setObject(1, Timestamp.from(LocalDateTime.parse(values[1], dtf).plusHours(toInt(values[0])).toInstant(ZoneOffset.UTC)));
                stmt.setObject(2, toInt(values[2]));
                stmt.setObject(2, toInt(values[3]));
                stmt.setObject(2, toInt(values[4]));
                stmt.setObject(2, toInt(values[5]));
                stmt.addBatch();

                if (batchCount == batchSize) {
                    batchCount = 0;
                    stmt.executeBatch();
                    System.out.println("execute Batch");
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
                System.err.println(line);
            }
        }

        System.out.println("execute last Batch");
        stmt.executeBatch();

        stmt.close();
        con.close();

        System.out.println("done");
    }

    public static Integer toInt(String s) {
        if(s == null || s.isEmpty() || s.isBlank()) {
            return null;
        } else {
            return Integer.parseInt(s);
        }
    }

}
