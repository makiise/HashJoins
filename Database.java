package fop.w10join;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Database {
    private static Path baseDataDirectory = Paths.get("data");

    public static void setBaseDataDirectory(Path baseDataDirectory) {
        Database.baseDataDirectory = baseDataDirectory;
    }

    private static Stream<String> readFileLines(String path) {
        try {
            return Files.lines(Paths.get(baseDataDirectory.toString() + "/" + path));
        } catch (Exception e) {
            e.printStackTrace();
            return Stream.empty();
        }
    }
    
    public static Stream<Customer> processInputFileCustomer() {
        try {
            return readFileLines("customer.tbl")
                .map(line -> {
                    String[] cols = line.split("\\|");
                    return new Customer(
                        Integer.parseInt(cols[1].split("#")[1]), 
                        cols[2].toCharArray(), 
                        Integer.parseInt(cols[3]), 
                        cols[4].toCharArray(), 
                        Float.parseFloat(cols[5]), 
                        cols[6], 
                        cols[7].toCharArray()
                    );
                });
        } catch (Exception e) {
            e.printStackTrace();
            return Stream.empty();
        }
    }

    public static Stream<LineItem> processInputFileLineItem() {
        try {
            return readFileLines("lineitem.tbl")
                .map(line -> {
                    String[] cols = line.split("\\|");
                    return new LineItem(
                        Integer.parseInt(cols[0]), 
                        Integer.parseInt(cols[1]), 
                        Integer.parseInt(cols[2]), 
                        Integer.parseInt(cols[3]), 
                        Integer.parseInt(cols[4]) * 100, 
                        Float.parseFloat(cols[5]), 
                        Float.parseFloat(cols[6]), 
                        Float.parseFloat(cols[7]),
                        cols[8].charAt(0),
                        cols[9].charAt(0),
                        LocalDate.parse(cols[10]), 
                        LocalDate.parse(cols[11]), 
                        LocalDate.parse(cols[12]), 
                        cols[13].toCharArray(), 
                        cols[14].toCharArray(), 
                        cols[15].toCharArray()
                    );
                });
        } catch (Exception e) {
            e.printStackTrace();
            return Stream.empty();
        }
    }

    public static Stream<Order> processInputFileOrders() {
        try {
            return readFileLines("orders.tbl")
                .map(line -> {
                    String[] cols = line.split("\\|");
                
                    return new Order(
                        Integer.parseInt(cols[0]), 
                        Integer.parseInt(cols[1]), 
                        cols[2].charAt(0), 
                        Float.parseFloat(cols[3]), 
                        LocalDate.parse(cols[4]), 
                        cols[5].toCharArray(), 
                        cols[6].toCharArray(), 
                        Integer.parseInt(cols[7]), 
                        cols[8].toCharArray()
                    );
                });
        } catch (Exception e) {
            e.printStackTrace();
            return Stream.empty();
        }
    }

    public Database() { }
    
    public long getAverageQuantityPerMarketSegment(String marketsegment) {
        Map<Integer, String> customerKeyToMarketSegment = processInputFileCustomer().collect(Collectors.toMap(a -> a.custKey, a -> a.mktsegment));
        Map<Integer, String> OrderKeyToMarketSegment = processInputFileOrders().collect(Collectors.toMap(a -> a.orderKey, a -> customerKeyToMarketSegment.get(a.custKey)));
        List<LineItem> quantitiesOfMarketSegment = processInputFileLineItem().filter(a -> OrderKeyToMarketSegment.get(a.orderKey).equals(marketsegment)).collect(Collectors.toList());
            
        long size = quantitiesOfMarketSegment.size();
        long sum = quantitiesOfMarketSegment.stream().collect(Collectors.summingLong(a -> a.quantity));
        return sum / size;
    }

    public static void main(String[] args) { }
}
