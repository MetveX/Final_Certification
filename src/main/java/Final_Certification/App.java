package Final_Certification;

import org.flywaydb.core.Flyway;
import java.sql.*;
import java.math.BigDecimal;
import java.util.Properties;

public class App {
    private static final String URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String USER = "postgres";
    private static final String PASSWORD = "";

    public static void main(String[] args) {
        System.out.println("Запуск системы управления заказами...");

        // Запускаем миграции БД
        runMigrations();

        // Демонстрируем CRUD операции
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false); // Начало транзакции

            try {
                // CREATE операции
                System.out.println("\n=== ОПЕРАЦИИ СОЗДАНИЯ ===");
                int newProductId = insertProduct(conn, "Новый Ноутбук", new BigDecimal("1299.99"), 15, "Electronics");
                int newCustomerId = insertCustomer(conn, "Сергей", "Петров", "+79991234567", "sergey@example.com");
                int newOrderId = createOrder(conn, newProductId, newCustomerId, 2, 1);

                // READ операции
                System.out.println("\n=== ОПЕРАЦИИ ЧТЕНИЯ ===");
                displayRecentOrders(conn);
                showPopularProducts(conn);

                // UPDATE операции
                System.out.println("\n=== ОПЕРАЦИИ ОБНОВЛЕНИЯ ===");
                updateProduct(conn, 1, new BigDecimal("1099.99"), 25);
                updateOrderStatus(conn, 1, 2);

                // DELETE операции
                System.out.println("\n=== ОПЕРАЦИИ УДАЛЕНИЯ ===");
                deleteOrder(conn, newOrderId);
                deleteCustomerWithoutOrders(conn, 5);

                // Коммит если все успешно
                conn.commit();
                System.out.println("\nВсе операции успешно завершены!");

            } catch (SQLException e) {
                System.out.println("Ошибка при выполнении операций: " + e.getMessage());
                conn.rollback();
                System.out.println("Транзакция откатана из-за ошибок.");
            }

        } catch (SQLException e) {
            System.out.println("Ошибка подключения к базе данных: " + e.getMessage());
        }
    }

    private static void runMigrations() {
        Flyway flyway = Flyway.configure()
                .dataSource(URL, USER, PASSWORD)
                .locations("classpath:db/migration")
                .load();
        flyway.migrate();
        System.out.println("Миграции базы данных успешно выполнены.");
    }

    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }

    // CREATE METHODS
    private static int insertProduct(Connection conn, String description, BigDecimal price, int quantity, String category) throws SQLException {
        String sql = "INSERT INTO product (description, price, quantity, category) VALUES (?, ?, ?, ?) RETURNING id";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, description);
            stmt.setBigDecimal(2, price);
            stmt.setInt(3, quantity);
            stmt.setString(4, category);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                int id = rs.getInt(1);
                System.out.println("Товар добавлен с ID: " + id);
                return id;
            }
            throw new SQLException("Не удалось добавить товар");
        }
    }

    private static int insertCustomer(Connection conn, String firstName, String lastName, String phone, String email) throws SQLException {
        String sql = "INSERT INTO customer (first_name, last_name, phone, email) VALUES (?, ?, ?, ?) RETURNING id";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, firstName);
            stmt.setString(2, lastName);
            stmt.setString(3, phone);
            stmt.setString(4, email);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                int id = rs.getInt(1);
                System.out.println("Клиент добавлен с ID: " + id);
                return id;
            }
            throw new SQLException("Не удалось добавить клиента");
        }
    }

    private static int createOrder(Connection conn, int productId, int customerId, int quantity, int statusId) throws SQLException {
        String sql = "INSERT INTO orders (product_id, customer_id, quantity, status_id) VALUES (?, ?, ?, ?) RETURNING id";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, productId);
            stmt.setInt(2, customerId);
            stmt.setInt(3, quantity);
            stmt.setInt(4, statusId);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                int id = rs.getInt(1);
                System.out.println("Заказ создан с ID: " + id);
                return id;
            }
            throw new SQLException("Не удалось создать заказ");
        }
    }

    // READ METHODS
    private static void displayRecentOrders(Connection conn) throws SQLException {
        String sql = "SELECT o.id, o.date_order, c.first_name, c.last_name, p.description, o.quantity, os.name as status " +
                "FROM orders o " +
                "JOIN customer c ON o.customer_id = c.id " +
                "JOIN product p ON o.product_id = p.id " +
                "JOIN order_status os ON o.status_id = os.id " +
                "ORDER BY o.date_order DESC LIMIT 5";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            System.out.println("Последние 5 заказов:");
            System.out.println("ID | Дата | Клиент | Товар  | Количество  | Статус");
            System.out.println("---|------|--------|--------|-------------|-------");

            while (rs.next()) {
                System.out.printf("%d | %s | %s %s | %s | %d | %s%n",
                        rs.getInt("id"),
                        rs.getTimestamp("date_order"),
                        rs.getString("first_name"),
                        rs.getString("last_name"),
                        rs.getString("description"),
                        rs.getInt("quantity"),
                        rs.getString("status"));
            }
        }
    }

    // UPDATE METHODS
    private static void updateProduct(Connection conn, int productId, BigDecimal newPrice, int newQuantity) throws SQLException {
        String sql = "UPDATE product SET price = ?, quantity = ? WHERE id = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setBigDecimal(1, newPrice);
            stmt.setInt(2, newQuantity);
            stmt.setInt(3, productId);

            int rows = stmt.executeUpdate();
            System.out.println(rows + " товар(ов) обновлен. ID: " + productId +
                    ", Новая цена: " + newPrice + ", Новое количество: " + newQuantity);
        }
    }

    private static void updateOrderStatus(Connection conn, int orderId, int newStatusId) throws SQLException {
        String sql = "UPDATE orders SET status_id = ? WHERE id = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, newStatusId);
            stmt.setInt(2, orderId);

            int rows = stmt.executeUpdate();
            System.out.println(rows + " обновлен заказ. ID: " + orderId +
                    ", Новый статус ID: " + newStatusId);
        }
    }

    // DELETE METHODS
    private static void deleteOrder(Connection conn, int orderId) throws SQLException {
        String sql = "DELETE FROM orders WHERE id = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, orderId);

            int rows = stmt.executeUpdate();
            if (rows > 0) {
                System.out.println("Заказ успешно удален. ID: " + orderId);
            } else {
                System.out.println("Заказ не найден с ID: " + orderId);
            }
        }
    }

    private static void deleteCustomerWithoutOrders(Connection conn, int customerId) throws SQLException {
        String sql = "DELETE FROM customer WHERE id = ? AND id NOT IN (SELECT DISTINCT customer_id FROM orders)";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, customerId);

            int rows = stmt.executeUpdate();
            if (rows > 0) {
                System.out.println("Клиент без заказов удален. ID: " + customerId);
            } else {
                System.out.println("Клиент с ID " + customerId + " не найден или имеет заказы");
            }
        }
    }

    // ANALYTICS METHODS
    private static void showPopularProducts(Connection conn) throws SQLException {
        String sql = "SELECT p.description, SUM(o.quantity) as total_ordered " +
                "FROM orders o JOIN product p ON o.product_id = p.id " +
                "GROUP BY p.id, p.description " +
                "ORDER BY total_ordered DESC LIMIT 3";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            System.out.println("\nТоп-3 самых популярных товаров:");
            while (rs.next()) {
                System.out.printf("Товар: %s, Количество заказов: %d%n",
                        rs.getString("description"),
                        rs.getInt("total_ordered"));
            }
        }
    }
}