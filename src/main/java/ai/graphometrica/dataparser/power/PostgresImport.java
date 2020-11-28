package ai.graphometrica.dataparser.power;

import org.postgresql.util.ReaderInputStream;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class PostgresImport {

    public static void main(String[] args) throws Exception {


        if(true) return;
        String connectionURL = "jdbc:postgresql://35.226.152.97:5432/minenergo";
        Class.forName("org.postgresql.Driver");
        Connection con = DriverManager.getConnection(connectionURL, "*", "*");

        File file = new File("output2.csv");    //creates a new file instance
        FileReader fr = new FileReader(file);   //reads the file
        BufferedReader br = new BufferedReader(fr);  //creates a buffering character input stream
        StringBuffer sb = new StringBuffer();    //constructs a string buffer with no characters
        String line;
        int batchCount = 0;
        int batchSize = 1000;

        String sql = "insert into minenergo.power(datetime, \"SubjectId\", \"PowerSystemId\", \"IBR_ActualConsumption\", \"IBR_ActualGeneration\", \"IBR_AveragePrice\", \"IBR_PlannedConsumption\", \"IBR_PlannedGeneration\", url)  values (?,?,?,?,?,?,?,?,?)";

        PreparedStatement stmt = con.prepareStatement(sql);


        while ((line = br.readLine()) != null) {
            try {
                var values = line.split(";");
                batchCount++;
                stmt.setTimestamp(1, Timestamp.from(LocalDateTime.parse(values[0]).toInstant(ZoneOffset.UTC)));
                stmt.setObject(2, toInt(values[1]));
                stmt.setObject(3, toInt(values[2]));
                stmt.setObject(4, toInt(values[3]));
                stmt.setObject(5, toInt(values[4]));
                stmt.setObject(6, toInt(values[5]));
                stmt.setObject(7, toInt(values[6]));
                stmt.setObject(8, toInt(values[7]));
                stmt.setString(9, values[8]);
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
