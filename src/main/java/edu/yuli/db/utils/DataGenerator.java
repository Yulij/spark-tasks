package edu.yuli.db.utils;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.ThreadLocalRandom;

import edu.yuli.db.ConnectionManager;

/**
 * Class DataGenerator
 *
 * Created by yslabko on 01/08/2018.
 */
public class DataGenerator {
    private static int maxProduct = 1000;
    private static int maxOrder = 200000;

    private static final String saveCategory = "INSERT INTO CATEGORY (NAME) VALUES (?)";
    private static final String saveOrder = "INSERT INTO `ORDER` (CUSTOMER_ID, CREATION_TMST, COMPLETION_TMST) VALUES (?, ?, ?)";
    private static final String saveOrderItem = "INSERT INTO ORDER_ITEM (PRODUCT_ID, QUANTITY, PROMOTION_ID, COST, ORDER_ID) VALUES (?, ?, ?, ?, ?)";
    private static final String saveProduct = "INSERT INTO `PRODUCT` (CATEGORY_ID, COST, BRAND) VALUES (?, ?, ?)";

    private static String[] categories = {"Electronics", "Instruments", "Phones", "Shoes", "Toys"};
    private static String[] toys = {"Fancy", "Lego", "Fisher-Price", "Barbie", "Monopoly" , "Gundam", "Hot Wheels"};
    private static String[] electronics = {"Intel", "HP", "Asus", "Acer", "Xiaomi" , "AMD", "Samsung", "LG"};
    private static String[] instruments = {"Bosch", "Makita", "Rioby", "AEG", "Einhell" , "Yato", "Hyundai", "Hitachi"};
    private static String[] phones = {"iPhone", "Samsung", "Xiaomi", "Huawei", "Philips" , "ZTE", "BlackBerry", "Motorola"};
    private static String[] shoes = {"Axes", "Rylko", "Nike", "Adidas", "Ecco" , "Basconi", "Ricker", "Caterpiller"};

    private static PreparedStatement psSaveCategory;
    private static PreparedStatement psSaveOrder;
    private static PreparedStatement psSaveOrderItem;
    private static PreparedStatement psSaveProduct;

    static {
        try {
            psSaveCategory = ConnectionManager.getConnection().prepareStatement(saveCategory, Statement.RETURN_GENERATED_KEYS);
            psSaveOrder = ConnectionManager.getConnection().prepareStatement(saveOrder, Statement.RETURN_GENERATED_KEYS);
            psSaveOrderItem = ConnectionManager.getConnection().prepareStatement(saveOrderItem, Statement.RETURN_GENERATED_KEYS);
            psSaveProduct = ConnectionManager.getConnection().prepareStatement(saveProduct, Statement.RETURN_GENERATED_KEYS);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
//        generateCategories();
//        generateProducts();
//        generateOrders();
        generateOrderItems();
    }

    public static void generateCategories() throws SQLException {
        for (String category : categories) {
            psSaveCategory.setString(1, category);
            psSaveCategory.executeUpdate();
        }
    }

    public static void generateProducts() throws SQLException {
        generateProduct(1, electronics);
        generateProduct(2, instruments);
        generateProduct(3, phones);
        generateProduct(4, shoes);
        generateProduct(5, toys);
    }

    private static void generateProduct(int categoryIndex, String[] product) throws SQLException {
        for (int i=0; i < maxProduct; i++) {
            psSaveProduct.setInt(1, categoryIndex);
            psSaveProduct.setDouble(2, ThreadLocalRandom.current().nextDouble(1.99, 999.99));
            psSaveProduct.setString(3, product[ThreadLocalRandom.current().nextInt(0, product.length)]);
            psSaveProduct.executeUpdate();
        }
    }

    private static void generateOrders() throws SQLException {
        long st = System.currentTimeMillis();
        for (int i=1; i <= maxOrder; i++) {
            LocalDateTime dateTime = LocalDateTime.now().minusMinutes(ThreadLocalRandom.current().nextInt(1, 500000));
            Date date = new Date(Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant()).getTime());
            psSaveOrder.setInt(1, ThreadLocalRandom.current().nextInt(1, 10001));
            psSaveOrder.setDate(2, date);
            psSaveOrder.setDate(3, date);
            psSaveOrder.addBatch();
            st = processBatch(psSaveOrder, i, st);
        }
        ConnectionManager.getConnection().commit();
        System.out.println("All records processed.");
    }

    private static void generateOrderItems() throws SQLException {
        long st = System.currentTimeMillis();
        for (int i=1; i <= maxOrder; i++) {
            int quantity = ThreadLocalRandom.current().nextInt(1, 3);
            double cost = ThreadLocalRandom.current().nextDouble(1.99, 999.99);
            psSaveOrderItem.setInt(1, getInt(1, maxProduct)*getInt(1, 5));
            psSaveOrderItem.setInt(2, quantity);
            psSaveOrderItem.setInt(3, getInt(1, 1000));
            psSaveOrderItem.setDouble(4, cost*quantity);
            psSaveOrderItem.setInt(5, i);
            psSaveOrderItem.addBatch();
            st = processBatch(psSaveOrderItem, i, st);
        }
        ConnectionManager.getConnection().commit();
        System.out.println("All records processed.");
    }

    private static int getInt(int start, int end) {
        return ThreadLocalRandom.current().nextInt(start, end + 1);
    }

    private static long processBatch(PreparedStatement ps, int i, long st) throws SQLException {
        if (i%10000 == 0) {
            int[] ints = ps.executeBatch();
            ConnectionManager.getConnection().commit();
            long en = System.currentTimeMillis();
            System.out.println("Processed in batch: " + ints.length + " records " + (en - st));

            st = en;
        }
        if (i%100000 == 0) {
            System.out.println("100 000 records processed. ");
        }

        return st;
    }

    private static void close(ResultSet rs) {
        try {
            if (rs != null)
                rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
