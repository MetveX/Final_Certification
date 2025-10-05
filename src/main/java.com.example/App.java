import org.flywaydb.core.Flyway;
import java.sql.*;
import java.math.BigDecimal;

public class App {
    private static final String URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String USER = "postgres";
    private static final String PASSWORD = "";

    public static void main(String[] args) {
        System.out.println("Starting Order Management System...");

        // Run database migrations
        runMigrations();

        // Demonstrate CRUD operations within transaction
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false); // Start transaction

            try {
                System.out.println("\n=== CREATE OPERATIONS ===");
                int newProductId = insertProduct(conn, "Игровая Мышь", new BigDecimal("45.99"), 50, "Электроника");
                int newCustomerId = insertCustomer(conn, "Алексей", "Волков", "+79147778899", "alexey.volkov@example.com");
                int newOrderId = createOrder(conn, newProductId, newCustomerId, 2, 1);

                System.out.println("\n=== READ OPERATIONS ===");
                displayRecentOrders(conn);

                System.out.println("\n=== UPDATE OPERATIONS ===");
                updateProduct(conn, 1, new BigDecimal("1099.99"), 25);

                System.out.println("\n=== DELETE OPERATIONS ===");
                deleteTestEntries(conn, newOrderId, newProductId, newCustomerId);

                conn.commit(); // Commit transaction on success
                System.out.println("\nAll operations completed successfully! Transaction committed.");

            } catch (SQLException e) {
                try {
                    conn.rollback(); // Rollback transaction on error
                } catch (SQLException ex) {
                    System.err.println("Error during rollback: " + ex.getMessage());
                }
                System.err.println("Error during operations: " + e.getMessage());
                System.out.println("Transaction rolled back due to errors.");
            }
        } catch (SQLException e) {
            System.err.println("Database connection error: " + e.getMessage());
        }
    }

    private static void runMigrations() {
        try {
            Flyway flyway = Flyway.configure()
                    .dataSource(URL, USER, PASSWORD)
                    .locations("classpath:db/migration")
                    .baselineOnMigrate(true)
                    .load();

            flyway.migrate();
            System.out.println("Database migrations completed successfully.");
        } catch (Exception e) {
            System.err.println("Error during migrations: " + e.getMessage());
            e.printStackTrace();
        }
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
            rs.next();
            int id = rs.getInt(1);
            System.out.println("Product inserted with ID: " + id);
            return id;
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
            rs.next();
            int id = rs.getInt(1);
            System.out.println("Customer inserted with ID: " + id);
            return id;
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
            rs.next();
            int id = rs.getInt(1);
            System.out.println("Order created with ID: " + id);
            return id;
        }
    }

    // READ METHOD
    private static void displayRecentOrders(Connection conn) throws SQLException {
        String sql = "SELECT o.id, o.date_order, c.first_name, c.last_name, p.description, o.quantity, os.name as status " +
                "FROM orders o " +
                "JOIN customer c ON o.customer_id = c.id " +
                "JOIN product p ON o.product_id = p.id " +
                "JOIN order_status os ON o.status_id = os.id " +
                "ORDER BY o.date_order DESC LIMIT 5";

        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            System.out.println("Last 5 orders:");
            System.out.printf("%-3s | %-19s | %-15s | %-20s | %-8s | %s%n",
                    "ID", "Date", "Customer", "Product", "Quantity", "Status");
            System.out.println("----------------------------------------------------------------------------------------");
            while (rs.next()) {
                System.out.printf("%-3d | %-19s | %-15s | %-20s | %-8d | %s%n",
                        rs.getInt("id"),
                        rs.getTimestamp("date_order"),
                        rs.getString("first_name") + " " + rs.getString("last_name"),
                        rs.getString("description"),
                        rs.getInt("quantity"),
                        rs.getString("status"));
            }
        }
    }

    // UPDATE METHOD
    private static void updateProduct(Connection conn, int productId, BigDecimal newPrice, int newQuantity) throws SQLException {
        String sql = "UPDATE product SET price = ?, quantity = ? WHERE id = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setBigDecimal(1, newPrice);
            stmt.setInt(2, newQuantity);
            stmt.setInt(3, productId);
            int rows = stmt.executeUpdate();
            System.out.println(rows + " product(s) updated. ID: " + productId +
                    ", New price: " + newPrice + ", New quantity: " + newQuantity);
        }
    }

    // DELETE METHOD
    private static void deleteTestEntries(Connection conn, int orderId, int productId, int customerId) throws SQLException {
        // Delete test order
        String deleteOrderSQL = "DELETE FROM orders WHERE id = ?";
        try (PreparedStatement stmt = conn.prepareStatement(deleteOrderSQL)) {
            stmt.setInt(1, orderId);
            int rows = stmt.executeUpdate();
            if (rows > 0) {
                System.out.println("Test order deleted successfully. ID: " + orderId);
            }
        }

        // Delete test product
        String deleteProductSQL = "DELETE FROM product WHERE id = ?";
        try (PreparedStatement stmt = conn.prepareStatement(deleteProductSQL)) {
            stmt.setInt(1, productId);
            int rows = stmt.executeUpdate();
            if (rows > 0) {
                System.out.println("Test product deleted successfully. ID: " + productId);
            }
        }

        // Delete test customer
        String deleteCustomerSQL = "DELETE FROM customer WHERE id = ?";
        try (PreparedStatement stmt = conn.prepareStatement(deleteCustomerSQL)) {
            stmt.setInt(1, customerId);
            int rows = stmt.executeUpdate();
            if (rows > 0) {
                System.out.println("Test customer deleted successfully. ID: " + customerId);
            }
        }
    }
}