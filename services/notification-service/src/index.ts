/**
 * Notification Service - Node.js/TypeScript
 *
 * Listens to all domain events from Kafka and dispatches notifications
 * via email, SMS, and push notifications. Implements idempotent consumption
 * and dead-letter queue handling for failed notifications.
 */

import express, { Request, Response } from "express";
import { Kafka, Consumer, Producer } from "kafkajs";
import helmet from "helmet";
import cors from "cors";
import morgan from "morgan";
import { v4 as uuidv4 } from "uuid";

// ─── Configuration ───────────────────────────────────────────────

interface Config {
  serviceName: string;
  port: string;
  kafkaBrokers: string;
  smtpHost?: string;
  smtpPort?: string;
  smtpUser?: string;
  smtpPass?: string;
  twilioSid?: string;
  twilioToken?: string;
  twilioPhone?: string;
  logLevel: string;
}

function loadConfig(): Config {
  return {
    serviceName: process.env.SERVICE_NAME || "notification-service",
    port: process.env.HTTP_PORT || "8084",
    kafkaBrokers: process.env.KAFKA_BROKERS || "localhost:9092",
    smtpHost: process.env.SMTP_HOST,
    smtpPort: process.env.SMTP_PORT,
    smtpUser: process.env.SMTP_USER,
    smtpPass: process.env.SMTP_PASS,
    twilioSid: process.env.TWILIO_SID,
    twilioToken: process.env.TWILIO_TOKEN,
    twilioPhone: process.env.TWILIO_PHONE,
    logLevel: process.env.LOG_LEVEL || "info",
  };
}

// ─── Types ───────────────────────────────────────────────────────

interface EventMetadata {
  event_id: string;
  correlation_id: string;
  event_type: string;
  source: string;
  timestamp: string;
  version: number;
}

interface BaseEvent {
  meta: EventMetadata;
  order_id?: string;
  user_id?: string;
}

interface OrderCreatedEvent extends BaseEvent {
  order_id: string;
  user_id: string;
  total_amount: number;
  currency: string;
  status: string;
}

interface OrderConfirmedEvent extends BaseEvent {
  order_id: string;
  user_id: string;
}

interface OrderCancelledEvent extends BaseEvent {
  order_id: string;
  user_id: string;
  reason: string;
}

interface PaymentProcessedEvent extends BaseEvent {
  payment_id: string;
  order_id: string;
  user_id: string;
  amount: number;
  currency: string;
  status: "pending" | "completed" | "failed" | "refunded";
  method: string;
  failure_reason?: string;
}

interface PaymentRefundedEvent extends BaseEvent {
  payment_id: string;
  order_id: string;
  user_id: string;
  amount: number;
  currency: string;
  reason: string;
}

interface InventoryReservedEvent extends BaseEvent {
  reservation_id: string;
  order_id: string;
  items: Array<{ sku: string; quantity: number }>;
  status: string;
}

interface NotificationPayload {
  notificationId: string;
  userId: string;
  orderId?: string;
  channel: "email" | "sms" | "push";
  subject: string;
  body: string;
  metadata: Record<string, unknown>;
}

interface NotificationLog {
  notificationId: string;
  eventId: string;
  eventType: string;
  channel: string;
  recipient: string;
  subject: string;
  status: "sent" | "failed" | "pending";
  error?: string;
  sentAt?: string;
  createdAt: string;
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
  debug: (msg: string, meta?: Record<string, unknown>) => {
    if (process.env.LOG_LEVEL === "debug") {
      console.debug(JSON.stringify({ level: "debug", time: new Date().toISOString(), msg, ...meta }));
    }
  },
};

// ─── Notification Providers ──────────────────────────────────────

interface NotificationProvider {
  send(notification: NotificationPayload, recipient: string): Promise<boolean>;
}

class ConsoleNotificationProvider implements NotificationProvider {
  async send(notification: NotificationPayload, recipient: string): Promise<boolean> {
    logger.info(`[NOTIFICATION] ${notification.channel.toUpperCase()} to ${recipient}`, {
      subject: notification.subject,
      bodyLength: notification.body.length,
      orderId: notification.orderId,
    });

    // In production, this would call actual email/SMS APIs
    console.log(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
    console.log(`📧 ${notification.channel.toUpperCase()} Notification`);
    console.log(`To: ${recipient}`);
    console.log(`Subject: ${notification.subject}`);
    console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
    console.log(notification.body);
    console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`);

    return true;
  }
}

class EmailProvider extends ConsoleNotificationProvider {
  constructor(private config: Config) {
    super();
  }

  async send(notification: NotificationPayload, recipient: string): Promise<boolean> {
    if (!this.config.smtpHost) {
      logger.warn("SMTP not configured, using console fallback");
      return super.send(notification, recipient);
    }

    try {
      // In production: use nodemailer to send actual emails
      // const transporter = createTransport({...});
      // await transporter.sendMail({...});
      logger.info("Email sent via SMTP", { to: recipient, subject: notification.subject });
      return true;
    } catch (err) {
      logger.error("Email sending failed", err);
      return false;
    }
  }
}

class SmsProvider extends ConsoleNotificationProvider {
  constructor(private config: Config) {
    super();
  }

  async send(notification: NotificationPayload, recipient: string): Promise<boolean> {
    if (!this.config.twilioSid) {
      logger.warn("Twilio not configured, using console fallback");
      return super.send(notification, recipient);
    }

    try {
      // In production: use Twilio SDK
      // await twilioClient.messages.create({...});
      logger.info("SMS sent via Twilio", { to: recipient });
      return true;
    } catch (err) {
      logger.error("SMS sending failed", err);
      return false;
    }
  }
}

// ─── Notification Service ────────────────────────────────────────

class NotificationService {
  private config: Config;
  private kafka: Kafka;
  private producer!: Producer;
  private consumer!: Consumer;
  private app: express.Application;
  private isShuttingDown = false;
  private processedEvents: Set<string> = new Set();
  private providers: Map<string, NotificationProvider> = new Map();
  private notificationLogs: NotificationLog[] = [];

  constructor(config: Config) {
    this.config = config;

    this.kafka = new Kafka({
      clientId: config.serviceName,
      brokers: config.kafkaBrokers.split(","),
      retry: { initialRetryTime: 100, retries: 8 },
    });

    this.app = express();
    this.setupMiddleware();
    this.setupRoutes();
    this.setupProviders();
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
    this.app.get("/notifications", this.listNotificationsHandler.bind(this));
    this.app.get("/notifications/:id", this.getNotificationHandler.bind(this));
    this.app.post("/notify", this.sendNotificationHandler.bind(this));
  }

  private setupProviders(): void {
    this.providers.set("email", new EmailProvider(this.config));
    this.providers.set("sms", new SmsProvider(this.config));
    this.providers.set("push", new ConsoleNotificationProvider());
  }

  // ─── HTTP Handlers ─────────────────────────────────────────────

  private async healthHandler(_req: Request, res: Response): Promise<void> {
    res.json({
      status: "healthy",
      service: this.config.serviceName,
      timestamp: new Date().toISOString(),
      version: "1.0.0",
      processedEvents: this.processedEvents.size,
      notificationsSent: this.notificationLogs.length,
    });
  }

  private async listNotificationsHandler(_req: Request, res: Response): Promise<void> {
    const page = parseInt(_req.query.page as string) || 1;
    const limit = parseInt(_req.query.limit as string) || 20;
    const start = (page - 1) * limit;
    const end = start + limit;

    res.json({
      notifications: this.notificationLogs.slice(start, end),
      total: this.notificationLogs.length,
      page,
      limit,
    });
  }

  private async getNotificationHandler(req: Request, res: Response): Promise<void> {
    const { id } = req.params;
    const log = this.notificationLogs.find(n => n.notificationId === id);

    if (!log) {
      res.status(404).json({ error: "Notification not found" });
      return;
    }

    res.json(log);
  }

  private async sendNotificationHandler(req: Request, res: Response): Promise<void> {
    const { userId, orderId, channel, subject, body, recipient } = req.body;

    if (!userId || !channel || !subject || !body || !recipient) {
      res.status(400).json({ error: "Missing required fields" });
      return;
    }

    const provider = this.providers.get(channel);
    if (!provider) {
      res.status(400).json({ error: `Unknown channel: ${channel}` });
      return;
    }

    const notification: NotificationPayload = {
      notificationId: `notif-${uuidv4()}`,
      userId,
      orderId,
      channel: channel as "email" | "sms" | "push",
      subject,
      body,
      metadata: req.body.metadata || {},
    };

    const success = await provider.send(notification, recipient);

    res.status(success ? 200 : 500).json({
      notificationId: notification.notificationId,
      status: success ? "sent" : "failed",
    });
  }

  // ─── Event Handlers ────────────────────────────────────────────

  private async handleOrderCreated(event: OrderCreatedEvent): Promise<void> {
    const { order_id, user_id, total_amount, currency } = event;

    logger.info("Handling OrderCreated notification", { orderId: order_id, userId: user_id });

    await this.sendNotification({
      userId: user_id,
      orderId: order_id,
      channel: "email",
      subject: `Order Confirmation #${order_id}`,
      body: `Thank you for your order!\n\nOrder ID: ${order_id}\nTotal: ${currency} ${total_amount.toFixed(2)}\n\nWe'll notify you when your order ships.`,
      metadata: { eventType: "OrderCreated", total: total_amount },
    });

    await this.publishNotificationEvent("OrderCreated", order_id, user_id, "email");
  }

  private async handleOrderConfirmed(event: OrderConfirmedEvent): Promise<void> {
    const { order_id, user_id } = event;

    logger.info("Handling OrderConfirmed notification", { orderId: order_id, userId: user_id });

    await this.sendNotification({
      userId: user_id,
      orderId: order_id,
      channel: "email",
      subject: `Your order #${order_id} is confirmed!`,
      body: `Great news! Your order has been confirmed and is being prepared for shipment.\n\nOrder ID: ${order_id}`,
      metadata: { eventType: "OrderConfirmed" },
    });

    await this.publishNotificationEvent("OrderConfirmed", order_id, user_id, "email");
  }

  private async handleOrderCancelled(event: OrderCancelledEvent): Promise<void> {
    const { order_id, user_id, reason } = event;

    logger.info("Handling OrderCancelled notification", { orderId: order_id, reason });

    await this.sendNotification({
      userId: user_id,
      orderId: order_id,
      channel: "email",
      subject: `Order #${order_id} Cancelled`,
      body: `Your order has been cancelled.\n\nOrder ID: ${order_id}\nReason: ${reason}\n\nIf you have any questions, please contact support.`,
      metadata: { eventType: "OrderCancelled", reason },
    });

    await this.publishNotificationEvent("OrderCancelled", order_id, user_id, "email");
  }

  private async handlePaymentProcessed(event: PaymentProcessedEvent): Promise<void> {
    const { order_id, user_id, amount, currency, status, failure_reason } = event;

    logger.info("Handling PaymentProcessed notification", { orderId: order_id, status });

    if (status === "completed") {
      await this.sendNotification({
        userId: user_id,
        orderId: order_id,
        channel: "email",
        subject: `Payment Successful - Order #${order_id}`,
        body: `Your payment of ${currency} ${amount.toFixed(2)} has been processed successfully.\n\nOrder ID: ${order_id}`,
        metadata: { eventType: "PaymentProcessed", status },
      });
    } else {
      await this.sendNotification({
        userId: user_id,
        orderId: order_id,
        channel: "email",
        subject: `Payment Failed - Order #${order_id}`,
        body: `We were unable to process your payment of ${currency} ${amount.toFixed(2)}.\n\nReason: ${failure_reason || "Unknown error"}\n\nPlease update your payment method and try again.`,
        metadata: { eventType: "PaymentProcessed", status, failureReason: failure_reason },
      });
    }

    await this.publishNotificationEvent("PaymentProcessed", order_id, user_id, "email");
  }

  private async handlePaymentRefunded(event: PaymentRefundedEvent): Promise<void> {
    const { order_id, user_id, amount, currency, reason } = event;

    logger.info("Handling PaymentRefunded notification", { orderId: order_id });

    await this.sendNotification({
      userId: user_id,
      orderId: order_id,
      channel: "email",
      subject: `Refund Processed - Order #${order_id}`,
      body: `A refund of ${currency} ${amount.toFixed(2)} has been processed.\n\nOrder ID: ${order_id}\nReason: ${reason}\n\nThe refund will appear in your account within 5-7 business days.`,
      metadata: { eventType: "PaymentRefunded", amount, reason },
    });

    await this.publishNotificationEvent("PaymentRefunded", order_id, user_id, "email");
  }

  private async handleInventoryReserved(event: InventoryReservedEvent): Promise<void> {
    const { order_id, items } = event;

    logger.info("Handling InventoryReserved notification", { orderId: order_id });

    const itemsList = items.map(i => `- ${i.sku}: ${i.quantity}`).join("\n");

    // This would typically go to warehouse/operations, not customer
    await this.sendNotification({
      userId: "warehouse",
      orderId: order_id,
      channel: "push",
      subject: `Stock Reserved for Order #${order_id}`,
      body: `Items reserved:\n${itemsList}`,
      metadata: { eventType: "InventoryReserved", itemCount: items.length },
    });

    if (order_id) {
      await this.publishNotificationEvent("InventoryReserved", order_id, "warehouse", "push");
    }
  }

  // ─── Notification Dispatch ─────────────────────────────────────

  private async sendNotification(payload: Omit<NotificationPayload, "notificationId">): Promise<void> {
    const notificationId = `notif-${uuidv4()}`;
    const fullPayload: NotificationPayload = { ...payload, notificationId };

    const provider = this.providers.get(payload.channel);
    if (!provider) {
      logger.warn("No provider for channel", { channel: payload.channel });
      return;
    }

    // In production, look up user email/phone from user service
    const recipient = this.getRecipientAddress(payload.userId, payload.channel);

    const startTime = Date.now();
    let success = false;
    let error: string | undefined;

    try {
      success = await provider.send(fullPayload, recipient);
    } catch (err) {
      error = String(err);
      logger.error("Notification send failed", err);
    }

    const log: NotificationLog = {
      notificationId,
      eventId: payload.metadata?.eventId as string || "manual",
      eventType: payload.metadata?.eventType as string || "manual",
      channel: payload.channel,
      recipient,
      subject: payload.subject,
      status: success ? "sent" : "failed",
      error,
      sentAt: success ? new Date().toISOString() : undefined,
      createdAt: new Date().toISOString(),
    };

    this.notificationLogs.push(log);

    // Keep only last 1000 logs in memory (use DB in production)
    if (this.notificationLogs.length > 1000) {
      this.notificationLogs = this.notificationLogs.slice(-1000);
    }

    logger.info(`Notification ${success ? "sent" : "failed"}`, {
      notificationId,
      channel: payload.channel,
      durationMs: Date.now() - startTime,
      recipient: recipient.replace(/(?<=.).*?(?=.@)/, "***"), // Mask email
    });
  }

  private getRecipientAddress(userId: string, channel: string): string {
    // In production: fetch from user profile service
    const mockAddresses: Record<string, Record<string, string>> = {
      "user-123": { email: "user123@example.com", sms: "+1234567890" },
      "warehouse": { email: "warehouse@company.com", push: "warehouse-device" },
    };

    const addresses = mockAddresses[userId];
    if (addresses) {
      return addresses[channel] || addresses["email"] || `${userId}@example.com`;
    }

    return `${userId}@example.com`;
  }

  // ─── Kafka Event Publishing ────────────────────────────────────

  private async publishNotificationEvent(
    eventType: string,
    orderId: string,
    userId: string,
    channel: string,
  ): Promise<void> {
    const event = {
      meta: {
        event_id: `evt-${uuidv4()}`,
        correlation_id: uuidv4(),
        event_type: "NotificationSent",
        source: this.config.serviceName,
        timestamp: new Date().toISOString(),
        version: 1,
      },
      notification_id: `notif-${uuidv4()}`,
      user_id: userId,
      order_id: orderId,
      channel,
      subject: `${eventType} notification`,
      body: `Automated notification for ${eventType}`,
      status: "sent",
    };

    try {
      await this.producer.send({
        topic: "notifications.sent",
        messages: [{ key: orderId, value: JSON.stringify(event) }],
      });
      logger.debug("Notification event published", { orderId, eventType });
    } catch (err) {
      logger.error("Failed to publish notification event", err);
    }
  }

  // ─── Kafka Consumer Setup ──────────────────────────────────────

  private async startKafkaConsumer(): Promise<void> {
    this.consumer = this.kafka.consumer({
      groupId: `${this.config.serviceName}-group`,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    });

    await this.consumer.connect();

    // Subscribe to all relevant topics
    await this.consumer.subscribe({
      topics: [
        "orders.created",
        "orders.confirmed",
        "orders.cancelled",
        "payments.processed",
        "payments.refunded",
        "inventory.reserved",
      ],
      fromBeginning: false,
    });

    logger.info("Kafka consumer subscribed to all notification topics");

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (this.isShuttingDown) return;

        try {
          const key = message.key?.toString() || "";
          const value = message.value?.toString() || "";

          if (!value) {
            logger.warn("Empty message received", { topic, partition, key });
            return;
          }

          const event = JSON.parse(value);
          const eventId = event.meta?.event_id;
          const eventType = event.meta?.event_type;

          logger.debug("Received Kafka message", { topic, partition, key, eventType });

          // Idempotency check
          if (eventId && this.processedEvents.has(eventId)) {
            logger.warn("Event already processed, skipping", { eventId, eventType });
            return;
          }

          // Route to appropriate handler
          await this.routeEvent(topic, event);

          // Mark as processed
          if (eventId) {
            this.processedEvents.add(eventId);
          }

          // Cleanup old processed events to prevent memory leak
          if (this.processedEvents.size > 10000) {
            const toDelete = Array.from(this.processedEvents).slice(0, 5000);
            toDelete.forEach(id => this.processedEvents.delete(id));
          }
        } catch (err) {
          logger.error("Error processing message", err);

          // In production: send to dead-letter queue
          await this.sendToDeadLetter(topic, message, err);
        }
      },
    });
  }

  private async routeEvent(topic: string, event: BaseEvent): Promise<void> {
    const eventType = event.meta?.event_type;

    switch (topic) {
      case "orders.created":
        await this.handleOrderCreated(event as OrderCreatedEvent);
        break;
      case "orders.confirmed":
        await this.handleOrderConfirmed(event as OrderConfirmedEvent);
        break;
      case "orders.cancelled":
        await this.handleOrderCancelled(event as OrderCancelledEvent);
        break;
      case "payments.processed":
        await this.handlePaymentProcessed(event as PaymentProcessedEvent);
        break;
      case "payments.refunded":
        await this.handlePaymentRefunded(event as PaymentRefundedEvent);
        break;
      case "inventory.reserved":
        await this.handleInventoryReserved(event as InventoryReservedEvent);
        break;
      default:
        logger.warn("Unknown topic", { topic, eventType });
    }
  }

  private async sendToDeadLetter(topic: string, message: unknown, error: unknown): Promise<void> {
    try {
      await this.producer.send({
        topic: "dead-letter",
        messages: [{
          key: `dlq-${uuidv4()}`,
          value: JSON.stringify({
            originalTopic: topic,
            originalMessage: message,
            error: String(error),
            timestamp: new Date().toISOString(),
            service: this.config.serviceName,
          }),
        }],
      });
      logger.info("Message sent to dead-letter queue", { topic });
    } catch (err) {
      logger.error("Failed to send to dead-letter queue", err);
    }
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
      logger.info("Kafka connections closed");
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

    // Start HTTP server
    const server = this.app.listen(this.config.port, () => {
      logger.info(`Notification Service listening on port ${this.config.port}`);
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
}

// ─── Main ────────────────────────────────────────────────────────

async function main(): Promise<void> {
  try {
    const config = loadConfig();
    const service = new NotificationService(config);
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
