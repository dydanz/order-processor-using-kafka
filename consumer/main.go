package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// Order represents the order message structure
type Order struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	ProductID  string    `json:"product_id"`
	Quantity   int       `json:"quantity"`
	Amount     float64   `json:"amount"`
	Timestamp  time.Time `json:"timestamp"`
}

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler
type ConsumerGroupHandler struct {
	ctx              context.Context
	processedCount   int64
	mu               sync.Mutex
	workerPool       int
	batchSize        int
	messageChannel   chan *sarama.ConsumerMessage
	wg               sync.WaitGroup
}

// NewConsumerGroupHandler creates a new consumer group handler with worker pool
func NewConsumerGroupHandler(ctx context.Context, workerPool, batchSize int) *ConsumerGroupHandler {
	handler := &ConsumerGroupHandler{
		ctx:            ctx,
		workerPool:     workerPool,
		batchSize:      batchSize,
		messageChannel: make(chan *sarama.ConsumerMessage, batchSize*workerPool),
	}

	// Start worker goroutines
	for i := 0; i < workerPool; i++ {
		handler.wg.Add(1)
		go handler.worker(i)
	}

	return handler
}

// Setup is called at the beginning of a new session, before ConsumeClaim
func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer group session started")
	return nil
}

// Cleanup is called at the end of a session, once all ConsumeClaim goroutines have exited
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer group session ended")
	return nil
}

// ConsumeClaim processes messages from a partition
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	batch := make([]*sarama.ConsumerMessage, 0, h.batchSize)

	for {
		select {
		case <-session.Context().Done():
			// Process remaining batch before exiting
			if len(batch) > 0 {
				h.processBatch(batch)
				session.MarkMessage(batch[len(batch)-1], "")
			}
			return nil

		case message := <-claim.Messages():
			if message == nil {
				continue
			}

			// Add message to batch
			batch = append(batch, message)

			// Process batch when full
			if len(batch) >= h.batchSize {
				h.processBatch(batch)
				session.MarkMessage(batch[len(batch)-1], "") // Commit offset
				batch = batch[:0]                            // Reset batch
			}
		}
	}
}

// processBatch sends batch to worker pool for parallel processing
func (h *ConsumerGroupHandler) processBatch(messages []*sarama.ConsumerMessage) {
	for _, msg := range messages {
		select {
		case h.messageChannel <- msg:
		case <-h.ctx.Done():
			return
		}
	}
}

// worker processes messages from the channel
func (h *ConsumerGroupHandler) worker(id int) {
	defer h.wg.Done()

	for {
		select {
		case msg := <-h.messageChannel:
			if msg == nil {
				continue
			}
			h.processMessage(msg)

		case <-h.ctx.Done():
			log.Printf("Worker %d shutting down", id)
			return
		}
	}
}

// processMessage handles individual message processing
func (h *ConsumerGroupHandler) processMessage(msg *sarama.ConsumerMessage) {
	var order Order

	// Unmarshal JSON
	if err := json.Unmarshal(msg.Value, &order); err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
		return
	}

	// Simulate processing (in production, this would be validation, inventory check, etc.)
	// For demonstration, we just log the order
	log.Printf("Processed order: ID=%s, Customer=%s, Product=%s, Quantity=%d, Amount=%.2f, Partition=%d, Offset=%d",
		order.OrderID, order.CustomerID, order.ProductID, order.Quantity, order.Amount,
		msg.Partition, msg.Offset)

	// Update counter
	h.mu.Lock()
	h.processedCount++
	if h.processedCount%1000 == 0 {
		log.Printf("Processed %d messages", h.processedCount)
	}
	h.mu.Unlock()
}

// Close stops all workers
func (h *ConsumerGroupHandler) Close() {
	close(h.messageChannel)
	h.wg.Wait()
}

func main() {
	// Read configuration from environment variables
	brokers := getEnvOrDefault("KAFKA_BROKERS", "kafka:9092")
	topic := getEnvOrDefault("KAFKA_TOPIC", "orders-placed")
	groupID := getEnvOrDefault("CONSUMER_GROUP_ID", "order-consumer-group")
	workerPool := 10   // Number of concurrent worker goroutines
	batchSize := 100   // Messages to batch before processing

	log.Printf("Starting consumer: brokers=%s, topic=%s, group=%s", brokers, topic, groupID)

	// Configure Sarama for high-throughput consumption
	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0                          // Kafka version
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetNewest     // Start from latest for new consumers
	config.Consumer.Offsets.AutoCommit.Enable = false         // Manual commit for better control
	config.Consumer.Fetch.Min = 1024 * 1024                   // 1MB minimum fetch size
	config.Consumer.Fetch.Default = 10 * 1024 * 1024          // 10MB default fetch
	config.Consumer.MaxProcessingTime = 1 * time.Second       // Max processing time before rebalance
	config.Consumer.Return.Errors = true
	config.Consumer.MaxWaitTime = 500 * time.Millisecond      // Max wait for min bytes
	config.Net.ReadTimeout = 30 * time.Second
	config.Net.WriteTimeout = 30 * time.Second

	// Create consumer group
	consumerGroup, err := sarama.NewConsumerGroup([]string{brokers}, groupID, config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	// Setup context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create handler
	handler := NewConsumerGroupHandler(ctx, workerPool, batchSize)
	defer handler.Close()

	// Error handling goroutine
	go func() {
		for err := range consumerGroup.Errors() {
			log.Printf("Consumer error: %v", err)
		}
	}()

	// Consume in goroutine
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop
			// When a rebalance happens, this function returns and needs to be called again
			if err := consumerGroup.Consume(ctx, []string{topic}, handler); err != nil {
				log.Printf("Error from consumer: %v", err)
			}

			// Check if context was cancelled
			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Println("Consumer started successfully")

	// Wait for interrupt signal
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigterm:
		log.Println("Received termination signal")
	case <-ctx.Done():
		log.Println("Context cancelled")
	}

	// Graceful shutdown
	log.Println("Shutting down consumer...")
	cancel()
	wg.Wait()

	log.Println("Consumer stopped")
}

// getEnvOrDefault returns environment variable or default value
func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
