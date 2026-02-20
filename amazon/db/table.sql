USE amazon;

-- 1. Warehouses (仓库)
-- 对应 world_amazon-1.proto 中的 AInitWarehouse
-- 存储仓库的 ID 和坐标 (x, y)
CREATE TABLE warehouses (
    warehouse_id INT PRIMARY KEY AUTO_INCREMENT,
    x INT NOT NULL,
    y INT NOT NULL
);

-- 2. Products (商品目录)
-- 对应 world_amazon-1.proto 中的 AProduct
-- 存储商品的基本信息
CREATE TABLE products (
    product_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    description VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) DEFAULT 0.00
);

-- 3. Inventory (库存)
-- 对应 APurchaseMore (进货) 和 APack (出货)
-- 记录每个仓库中每种商品的数量
CREATE TABLE inventory (
    warehouse_id INT,
    product_id BIGINT,
    count INT DEFAULT 0,
    PRIMARY KEY (warehouse_id, product_id),
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
);

-- 4. Orders / Packages (订单/包裹)
-- 这是核心表，连接了 User, World 和 UPS 的所有状态
-- 对应 APack (World), PickupRequest (UPS), APackage (查询)
CREATE TABLE orders (
    order_id BIGINT AUTO_INCREMENT PRIMARY KEY, -- Amazon 内部订单号
    package_id BIGINT UNIQUE, -- 发送给 World 和 UPS 的 ID (shipid/package_id)
    
    -- 客户信息
    ups_user_id VARCHAR(255), -- 对应 PickupRequest.ups_user_id
    dest_x INT NOT NULL,      -- 对应 PickupRequest.user_destination
    dest_y INT NOT NULL,
    
    -- 仓库信息
    warehouse_id INT,
    
    -- UPS 运输信息
    truck_id INT DEFAULT NULL, -- 对应 PickupResp.truck_id, TruckArrived.truck_id
    
    -- 状态追踪
    -- 状态流转: CREATED -> PACKING -> PACKED -> LOADING -> LOADED -> DELIVERING -> DELIVERED
    status VARCHAR(50) DEFAULT 'CREATED', 
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (warehouse_id) REFERENCES warehouses(warehouse_id)
);

-- 5. Order Items (订单包含的商品)
-- 对应 APack.things 和 PickupRequest.items
-- 记录每个订单里包含哪些商品和数量
CREATE TABLE order_items (
    order_id BIGINT,
    product_id BIGINT,
    quantity INT NOT NULL,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- 初始化索引以优化查询速度
CREATE INDEX idx_order_package ON orders(package_id);
CREATE INDEX idx_order_status ON orders(status);