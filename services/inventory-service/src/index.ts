/**
 * Inventory Service - Node.js/TypeScript
 *
 * Manages stock levels, reservations, and releases. Uses Redis as the primary
 * data store for fast read/write operations and Kafka for event-driven
 * communication with other services in the saga.
 */

import express, { Request, Response, NextFunction } from "express";
import { Kafka, Consumer, Producer } from "kafkajs";
import { Redis } from "ioredis";
import helmet from "helmet";
import cors from "cors";
import morgan from "morgan";
import { v4 as uuidv4 } from "uuid";

// ─── Configuration ───────────────────────────────────────────────

interface Config {
  serviceName: string;
  port: string;
  redisHost: string;
  redisPort: string;
  kafkaBrokers: string;
  logLevel: string;
}

function loadConfig(): Config {
  return {
    serviceName: process.env.SERVICE_NAME || "inventory-service",
    port: process.env.HTTP_PORT || "8083",
    redisHost: process.env.REDIS_HOST || "localhost",
    redisPort: process.env.REDIS_PORT || "6379",
    kafkaBrokers: process.env.KAFKA_BROKERS || "localhost:9092",
    logLevel: process.env.LOG_LEVEL || "info",
  };
}

// ─── Types ───────────────────────────────────────────────────────

interface InventoryItem {
  sku: string;
  name: string;
  quantity: number;
  reserved: number;
  updatedAt: string;
}

interface OrderItem {
  sku: string;
  name?: string;
  quantity: number;
  price: number;
}

interface OrderCreatedEvent {
  meta: EventMetadata;
  order_id: string;
  user_id: string;
  items: OrderItem[];
  total_amount: number;
  currency: string;
  status: string;
  shipping_address: unknown;
}

interface OrderCancelledEvent {
  meta: EventMetadata;
  order_id: string;
  user_id: string;
  reason: string;
}

interface PaymentProcessedEvent {
  meta: EventMetadata;
  payment_id: string;
  order_id: string;
  user_id: string;
  amount: number;
  currency: string;
  status: "pending" | "completed" | "failed" | "refunded";
  method: string;
  failure_reason?: string;
}

interface EventMetadata {
  event_id: string;
  correlation_id: string;
  event_type: string;
  source: string;
  timestamp: string;
  version: number;
}

interface Reservation {
  reservationId: string;
  orderId: string;
  items: OrderItem[];
  status: "reserved" | "released" | "committed";
  createdAt: string;
  updatedAt: string;
}

// ─── Logger ──────────────────────────────────────────────────────

const logger = {
  info: (msg: string, meta?: Record<string, unknown>) => {
    console.log(JSON.stringify({ level: "info", time: new Date().toISOString(), msg, ...meta }));
  },
  error: (msg: string, err?: unknown) => {
    console.error(JSON.stringify({ level: "error", time: new Date().toISOString(), msg, error: String(err) }));
  },
  warn: (msg: string, meta?: Record<string, unknown>) => {
    console.warn(JSON.stringify({ level: "warn", time: new Date().toISOString(), msg, ...meta }));
  },
};

// ─── Inventory Service Class ─────────────────────────────────────

class InventoryService {
  private config: Config;
  private redis: Redis;
  private kafka: Kafka;
  private producer!: Producer;
  private consumer!: Consumer;
  private app: express.Application;
  private isShuttingDown = false;
  private processedEvents: Set<string> = new Set();

  constructor(config: Config) {
    this.config = config;

    // Redis connection
    this.redis = new Redis({
      host: config.redisHost,
      port: parseInt(config.redisPort, 10),
      retryStrategy: (times: number) => Math.min(times * 50, 2000),
      maxRetriesPerRequest: 3,
    });

    // Kafka connection
    this.kafka = new Kafka({
      clientId: config.serviceName,
      brokers: config.kafkaBrokers.split(","),
      retry: { initialRetryTime: 100, retries: 8 },
    });

    this.app = express();
    this.setupMiddleware();
    this.setupRoutes();
    this.setupEventHandlers();
  }

  // ─── Setup ─────────────────────────────────────────────────────

  private setupMiddleware(): void {
    this.app.use(helmet());
    this.app.use(cors());
    this.app.use(express.json());
    this.app.use(morgan("combined"));
  }

  private setupRoutes(): void {
    this.app.get("/health", this.healthHandler.bind(this));
    this.app.get("/inventory/:sku", this.getStockHandler.bind(this));
    this.app.put("/inventory/:sku", this.updateStockHandler.bind(this));
    this.app.post("/inventory/:sku/reserve", this.reserveStockHandler.bind(this));
    this.app.post("/inventory/:sku/release", this.releaseStockHandler.bind(this));
    this.app.get("/inventory", this.listInventoryHandler.bind(this));
  }

  private setupEventHandlers(): void {
    this.redis.on("connect", () => logger.info("Connected to Redis"));
    this.redis.on("error", (err) => logger.error("Redis error", err));
  }

  // ─── HTTP Handlers ─────────────────────────────────────────────

  private async healthHandler(_req: Request, res: Response): Promise<void> {
    const redisStatus = this.redis.status === "ready" ? "connected" : "disconnected";

    res.json({
      status: "healthy",
      service: this.config.serviceName,
      redis: redisStatus,
      timestamp: new Date().toISOString(),
      version: "1.0.0",
    });
  }

  private async getStockHandler(req: Request, res: Response): Promise<void> {
    try {
      const { sku } = req.params;
      const stock = await this.getStock(sku);

      if (stock === null) {
        res.status(404).json({ error: "SKU not found" });
        return;
      }

      res.json({ sku, ...stock });
    } catch (err) {
      logger.error("Error getting stock", err);
      res.status(500).json({ error: "Internal server error" });
    }
  }

  private async updateStockHandler(req: Request, res: Response): Promise<void> {
    try {
      const { sku } = req.params;
      const { quantity, name } = req.body;

      if (typeof quantity !== "number" || quantity < 0) {
        res.status(400).json({ error: "Invalid quantity" });
        return;
      }

      const item = await this.updateStock(sku, quantity, name);
      logger.info("Stock updated", { sku, quantity, name });

      res.json(item);
    } catch (err) {
      logger.error("Error updating stock", err);
      res.status(500).json({ error: "Internal server error" });
    }
  }

  private async reserveStockHandler(req: Request, res: Response): Promise<void> {
    try {
      const { sku } = req.params;
      const { quantity, orderId } = req.body;

      if (!quantity || !orderId) {
        res.status(400).json({ error: "quantity and orderId are required" });
        return;
      }

      const success = await this.reserveStock(sku, quantity);

      if (!success) {
        res.status(409).json({ error: "Insufficient stock", sku });
        return;
      }

      res.json({ success: true, sku, quantity, orderId });
    } catch (err) {
      logger.error("Error reserving stock", err);
      res.status(500).json({ error: "Internal server error" });
    }
  }

  private async releaseStockHandler(req: Request, res: Response): Promise<void> {
    try {
      const { sku } = req.params;
      const { quantity } = req.body;

      if (!quantity) {
        res.status(400).json({ error: "quantity is required" });
        return;
      }

      await this.releaseStock(sku, quantity);

      res.json({ success: true, sku, quantity, message: "Stock released" });
    } catch (err) {
      logger.error("Error releasing stock", err);
      res.status(500).json({ error: "Internal server error" });
    }
  }

  private async listInventoryHandler(_req: Request, res: Response): Promise<void> {
    try {
      const keys = await this.redis.keys("inventory:*");
      const items: InventoryItem[] = [];

      for (const key of keys) {
        const data = await this.redis.get(key);
        if (data) {
          items.push(JSON.parse(data));
        }
      }

      res.json({ items, total: items.length });
    } catch (err) {
      logger.error("Error listing inventory", err);
      res.status(500).json({ error: "Internal server error" });
    }
  }

  // ─── Business Logic ────────────────────────────────────────────

  private async getStock(sku: string): Promise<{ quantity: number; reserved: number; available: number; name: string } | null> {
    const data = await this.redis.get(`inventory:${sku}`);
    if (!data) return null;

    const item: InventoryItem = JSON.parse(data);
    return {
      quantity: item.quantity,
      reserved: item.reserved,
      available: item.quantity - item.reserved,
      name: item.name,
    };
  }

  private async updateStock(sku: string, quantity: number, name?: string): Promise<InventoryItem> {
    const existing = await this.redis.get(`inventory:${sku}`);
    let item: InventoryItem;

    if (existing) {
      item = JSON.parse(existing);
      item.quantity = quantity;
      if (name) item.name = name;
    } else {
      item = {
        sku,
        name: name || sku,
        quantity,
        reserved: 0,
        updatedAt: new Date().toISOString(),
      };
    }

    item.updatedAt = new Date().toISOString();
    await this.redis.set(`inventory:${sku}`, JSON.stringify(item));

    return item;
  }

  private async reserveStock(sku: string, quantity: number): Promise<boolean> {
    const key = `inventory:${sku}`;

    // Use Lua script for atomic stock reservation
    const luaScript = `
      local key = KEYS[1]
      local qty = tonumber(ARGV[1])
      local data = redis.call('get', key)

      if not data then
        return {-1, "SKU not found"}
      end

      local item = cjson.decode(data)
      local available = item.quantity - item.reserved

      if available < qty then
        return {0, "Insufficient stock"}
      end

      item.reserved = item.reserved + qty
      item.updatedAt = ARGV[2]
      redis.call('set', key, cjson.encode(item))

      return {1, item.reserved}
    `;

    try {
      const result = await this.redis.eval(luaScript, 1, key, quantity, new Date().toISOString()) as [number, string];
      return result[0] === 1;
    } catch {
      // Fallback if Lua script fails (RedisJSON not available)
      const data = await this.redis.get(key);
      if (!data) return false;

      const item: InventoryItem = JSON.parse(data);
      const available = item.quantity - item.reserved;

      if (available < quantity) return false;

      item.reserved += quantity;
      item.updatedAt = new Date().toISOString();
      await this.redis.set(key, JSON.stringify(item));

      return true;
    }
  }

  private async releaseStock(sku: string, quantity: number): Promise<void> {
    const key = `inventory:${sku}`;
    const data = await this.redis.get(key);

    if (!data) return;

    const item: InventoryItem = JSON.parse(data);
    item.reserved = Math.max(0, item.reserved - quantity);
    item.updatedAt = new Date().toISOString();

    await this.redis.set(key, JSON.stringify(item));
  }

  private async commitReservation(sku: string, quantity: number): Promise<void> {
    const key = `inventory:${sku}`;
    const data = await this.redis.get(key);

    if (!data) return;

    const item: InventoryItem = JSON.parse(data);
    item.quantity -= quantity;
    item.reserved = Math.max(0, item.reserved - quantity);
    item.updatedAt = new Date().toISOString();

    await this.redis.set(key, JSON.stringify(item));
  }

  // ─── Kafka Event Handlers ──────────────────────────────────────

  private async handleOrderCreated(event: OrderCreatedEvent): Promise<void> {
    const { order_id, items } = event;

    logger.info("Processing OrderCreated for inventory reservation", { orderId: order_id });

    // Check idempotency
    if (this.processedEvents.has(event.meta.event_id)) {
      logger.warn("Event already processed, skipping", { eventId: event.meta.event_id });
      return;
    }

    const reservationId = `res-${uuidv4()}`;
    const failedItems: { sku: string; requested: number; available: number }[] = [];
    const reservedItems: { sku: string; quantity: number }[] = [];

    // Try to reserve stock for each item
    for (const item of items) {
      const stock = await this.getStock(item.sku);

      if (!stock) {
        failedItems.push({ sku: item.sku, requested: item.quantity, available: 0 });
        continue;
      }

      if (stock.available < item.quantity) {
        failedItems.push({ sku: item.sku, requested: item.quantity, available: stock.available });
        continue;
      }

      const success = await this.reserveStock(item.sku, item.quantity);
      if (success) {
        reservedItems.push({ sku: item.sku, quantity: item.quantity });
      } else {
        failedItems.push({ sku: item.sku, requested: item.quantity, available: stock.available });
      }
    }

    // Store reservation
    const reservation: Reservation = {
      reservationId,
      orderId: order_id,
      items: reservedItems,
      status: failedItems.length > 0 ? "released" : "reserved",
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };

    await this.redis.set(`reservation:${order_id}`, JSON.stringify(reservation));

    if (failedItems.length > 0) {
      logger.warn("Inventory reservation failed, releasing reserved items", { orderId: order_id, failedItems });

      // Release any reserved items
      for (const item of reservedItems) {
        await this.releaseStock(item.sku, item.quantity);
      }

      // Publish inventory release event (saga compensation)
      await this.publishEvent("inventory.released", order_id, {
        meta: this.createEventMetadata("InventoryReleased"),
        reservation_id: reservationId,
        order_id,
        items: reservedItems,
        reason: `insufficient_stock: ${failedItems.map(f => f.sku).join(",")}`,
      });

      return;
    }

    logger.info("Inventory reserved successfully", { orderId: order_id, reservationId, items: reservedItems });

    // Publish inventory reserved event
    await this.publishEvent("inventory.reserved", order_id, {
      meta: this.createEventMetadata("InventoryReserved"),
      reservation_id: reservationId,
      order_id,
      items: reservedItems,
      status: "reserved",
    });

    this.processedEvents.add(event.meta.event_id);
  }

  private async handleOrderCancelled(event: OrderCancelledEvent): Promise<void> {
    const { order_id } = event;

    logger.info("Processing OrderCancelled, releasing inventory", { orderId: order_id });

    const reservationData = await this.redis.get(`reservation:${order_id}`);
    if (!reservationData) {
      logger.warn("No reservation found for order", { orderId: order_id });
      return;
    }

    const reservation: Reservation = JSON.parse(reservationData);

    // Release all reserved stock
    for (const item of reservation.items) {
      await this.releaseStock(item.sku, item.quantity);
      logger.info("Stock released", { sku: item.sku, quantity: item.quantity, orderId: order_id });
    }

    reservation.status = "released";
    reservation.updatedAt = new Date().toISOString();
    await this.redis.set(`reservation:${order_id}`, JSON.stringify(reservation));

    // Publish inventory released event
    await this.publishEvent("inventory.released", order_id, {
      meta: this.createEventMetadata("InventoryReleased"),
      reservation_id: reservation.reservationId,
      order_id,
      items: reservation.items,
      reason: event.reason || "order_cancelled",
    });
  }

  private async handlePaymentProcessed(event: PaymentProcessedEvent): Promise<void> {
    const { order_id, status } = event;

    if (status !== "completed") {
      logger.info("Payment not completed, skipping inventory commit", { orderId: order_id, paymentStatus: status });
      return;
    }

    logger.info("Payment completed, committing inventory reservation", { orderId: order_id });

    const reservationData = await this.redis.get(`reservation:${order_id}`);
    if (!reservationData) {
      logger.warn("No reservation found for order to commit", { orderId: order_id });
      return;
    }

    const reservation: Reservation = JSON.parse(reservationData);

    // Commit: reduce actual quantity and release reservation
    for (const item of reservation.items) {
      await this.commitReservation(item.sku, item.quantity);
      logger.info("Inventory committed", { sku: item.sku, quantity: item.quantity, orderId: order_id });
    }

    reservation.status = "committed";
    reservation.updatedAt = new Date().toISOString();
    await this.redis.set(`reservation:${order_id}`, JSON.stringify(reservation));
  }

  // ─── Kafka Producer ────────────────────────────────────────────

  private async publishEvent(topic: string, key: string, event: unknown): Promise<void> {
    try {
      await this.producer.send({
        topic,
        messages: [{ key, value: JSON.stringify(event) }],
      });
      logger.info("Event published", { topic, key });
    } catch (err) {
      logger.error("Failed to publish event", err);
      // In production, retry or save to dead-letter queue
    }
  }

  private createEventMetadata(eventType: string): EventMetadata {
    return {
      event_id: `evt-${uuidv4()}`,
      correlation_id: uuidv4(),
      event_type: eventType,
      source: this.config.serviceName,
      timestamp: new Date().toISOString(),
      version: 1,
    };
  }

  // ─── Kafka Consumer Setup ──────────────────────────────────────

  private async startKafkaConsumer(): Promise<void> {
    this.consumer = this.kafka.consumer({
      groupId: `${this.config.serviceName}-group`,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    });

    await this.consumer.connect();

    await this.consumer.subscribe({
      topics: ["orders.created", "orders.cancelled", "payments.processed"],
      fromBeginning: false,
    });

    logger.info("Kafka consumer subscribed to topics", {
      topics: ["orders.created", "orders.cancelled", "payments.processed"],
    });

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (this.isShuttingDown) return;

        try {
          const key = message.key?.toString() || "";
          const value = message.value?.toString() || "";
          const event = JSON.parse(value);

          logger.info("Received Kafka message", { topic, partition, key, eventType: event.meta?.event_type });

          switch (topic) {
            case "orders.created":
              await this.handleOrderCreated(event as OrderCreatedEvent);
              break;
            case "orders.cancelled":
              await this.handleOrderCancelled(event as OrderCancelledEvent);
              break;
            case "payments.processed":
              await this.handlePaymentProcessed(event as PaymentProcessedEvent);
              break;
            default:
              logger.warn("Unknown topic", { topic });
          }
        } catch (err) {
          logger.error("Error processing message", err);
          // In production: send to dead-letter queue
        }
      },
    });
  }

  // ─── Graceful Shutdown ─────────────────────────────────────────

  private async shutdown(signal: string): Promise<void> {
    logger.info(`Received ${signal}, shutting down gracefully...`);
    this.isShuttingDown = true;

    const shutdownTimeout = setTimeout(() => {
      logger.error("Forced shutdown due to timeout");
      process.exit(1);
    }, 30000);

    try {
      await this.consumer?.disconnect();
      await this.producer?.disconnect();
      await this.redis.quit();
      logger.info("All connections closed");
    } catch (err) {
      logger.error("Error during shutdown", err);
    }

    clearTimeout(shutdownTimeout);
    process.exit(0);
  }

  // ─── Start ─────────────────────────────────────────────────────

  async start(): Promise<void> {
    // Connect to Kafka producer
    this.producer = this.kafka.producer({
      allowAutoTopicCreation: true,
      transactionTimeout: 30000,
    });
    await this.producer.connect();
    logger.info("Kafka producer connected");

    // Start Kafka consumer
    await this.startKafkaConsumer();

    // Seed initial inventory data
    await this.seedInventory();

    // Start HTTP server
    const server = this.app.listen(this.config.port, () => {
      logger.info(`Inventory Service listening on port ${this.config.port}`);
    });

    // Graceful shutdown
    const shutdownHandler = (signal: string) => {
      server.close(async () => {
        await this.shutdown(signal);
      });
    };

    process.on("SIGINT", () => shutdownHandler("SIGINT"));
    process.on("SIGTERM", () => shutdownHandler("SIGTERM"));
  }

  private async seedInventory(): Promise<void> {
    const initialStock: Array<{ sku: string; name: string; quantity: number }> = [
      { sku: "PROD-001", name: "Wireless Headphones", quantity: 100 },
      { sku: "PROD-002", name: "Smart Watch", quantity: 50 },
      { sku: "PROD-003", name: "USB-C Cable", quantity: 200 },
      { sku: "PROD-004", name: "Bluetooth Speaker", quantity: 75 },
      { sku: "PROD-005", name: "Phone Case", quantity: 500 },
    ];

    for (const item of initialStock) {
      const exists = await this.redis.exists(`inventory:${item.sku}`);
      if (!exists) {
        await this.updateStock(item.sku, item.quantity, item.name);
        logger.info("Seeded inventory", { sku: item.sku, name: item.name, quantity: item.quantity });
      }
    }
  }
}

// ─── Error Handling Middleware ───────────────────────────────────

function errorHandler(err: Error, _req: Request, res: Response, _next: NextFunction): void {
  logger.error("Unhandled error", err);
  res.status(500).json({ error: "Internal server error" });
}

// ─── Main ────────────────────────────────────────────────────────

async function main(): Promise<void> {
  try {
    const config = loadConfig();
    const service = new InventoryService(config);
    await service.start();
  } catch (err) {
    logger.error("Failed to start service", err);
    process.exit(1);
  }
}

// Handle unhandled errors
process.on("uncaughtException", (err) => {
  logger.error("Uncaught exception", err);
  process.exit(1);
});

process.on("unhandledRejection", (reason) => {
  logger.error("Unhandled rejection", reason);
});

main();
