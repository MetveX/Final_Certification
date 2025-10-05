-- =============================================
-- ЗАПРОСЫ НА ЧТЕНИЕ (SELECT)
-- =============================================

-- 1. Список всех заказов за последние 7 дней с именем покупателя и описанием товара
SELECT
    o.id AS order_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    p.description AS product_name,
    o.quantity,
    o.date_order,
    os.name AS status
FROM orders o
JOIN customer c ON o.customer_id = c.id
JOIN product p ON o.product_id = p.id
JOIN order_status os ON o.status_id = os.id
WHERE o.date_order >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY o.date_order DESC;

-- 2. Топ-3 самых популярных товара (по общему количеству заказанных единиц)
SELECT
    p.id,
    p.description,
    SUM(o.quantity) AS total_ordered
FROM orders o
JOIN product p ON o.product_id = p.id
GROUP BY p.id, p.description
ORDER BY total_ordered DESC
LIMIT 3;

-- 3. Клиенты с самой высокой общей суммой заказов
SELECT
    c.id,
    c.first_name || ' ' || c.last_name AS customer_name,
    SUM(o.quantity * p.price) AS total_spent
FROM orders o
JOIN customer c ON o.customer_id = c.id
JOIN product p ON o.product_id = p.id
WHERE os.name IN ('Оплачен', 'Доставлен') -- Учитываем только завершенные заказы
GROUP BY c.id, customer_name
ORDER BY total_spent DESC;

-- 4. Ежемесячная статистика продаж по категориям товаров
SELECT
    DATE_TRUNC('month', o.date_order) AS month,
    p.category,
    COUNT(o.id) AS orders_count,
    SUM(o.quantity) AS total_quantity,
    SUM(o.quantity * p.price) AS total_revenue
FROM orders o
JOIN product p ON o.product_id = p.id
GROUP BY month, p.category
ORDER BY month DESC, total_revenue DESC;

-- 5. Товары, которые нуждаются в пополнении запасов (количество на складе меньше 10)
SELECT
    id,
    description,
    quantity,
    category
FROM product
WHERE quantity < 10
ORDER BY quantity ASC;

-- =============================================
-- ЗАПРОСЫ НА ИЗМЕНЕНИЕ (UPDATE)
-- =============================================

-- 6. Обновление количества товара на складе после заказа
UPDATE product
SET quantity = quantity - 5
WHERE id = 1;

-- 7. Повышение цены на 10% для товаров категории "Электроника"
UPDATE product
SET price = price * 1.10
WHERE category = 'Электроника';

-- 8. Обновление статуса заказа на "Доставлен" для заказов старше 3 дней
UPDATE orders
SET status_id = (SELECT id FROM order_status WHERE name = 'Доставлен')
WHERE status_id = (SELECT id FROM order_status WHERE name = 'Отгружен')
AND date_order < CURRENT_DATE - INTERVAL '3 days';

-- =============================================
-- ЗАПРОСЫ НА УДАЛЕНИЕ (DELETE)
-- =============================================

-- 9. Удаление клиентов без заказов
DELETE FROM customer
WHERE id NOT IN (SELECT DISTINCT customer_id FROM orders);

-- 10. Удаление устаревших отмененных заказов (старше 1 года)
DELETE FROM orders
WHERE status_id = (SELECT id FROM order_status WHERE name = 'Отменен')
AND date_order < CURRENT_DATE - INTERVAL '1 year';