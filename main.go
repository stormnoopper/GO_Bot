package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/bwmarrin/discordgo"
	"github.com/google/generative-ai-go/genai"
	"github.com/segmentio/kafka-go"
	"google.golang.org/api/option"
)

type KafkaMessage struct {
	ChannelID string `json:"channel_id"`
	Content   []byte `json:"content"`
}

func main() {
	// Initialize the Discord session
	sess, err := discordgo.New("DISCORD_TOKEN")
	if err != nil {
		log.Fatal(err)
	}

	// Initialize the Gemini client
	ctx := context.Background()
	client, err := genai.NewClient(ctx, option.WithAPIKey("GEMINI_API_KEY"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Kafka setup
	kafkaURL := os.Getenv("KAFKA_BROKER_ADDRESS")
	topic := "discord_messages"
	partition := 0

	// Create a new context
	kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaURL},
		Topic:   topic,
	})
	defer kafkaWriter.Close()

	// Kafka reader setup
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaURL},
		Topic:     topic,
		Partition: partition,
	})
	defer kafkaReader.Close()

	sess.AddHandler(func(s *discordgo.Session, m *discordgo.MessageCreate) {
		if m.Author.ID == s.State.User.ID {
			return
		}

		if strings.HasPrefix(m.Content, "/B ") {
			cutText := m.Content[3:]

			// Call the Gemini API
			model := client.GenerativeModel("gemini-1.5-flash")
			resp, err := model.GenerateContent(ctx, genai.Text(cutText))
			if err != nil {
				log.Println("Error generating content:", err)
				s.ChannelMessageSend(m.ChannelID, "Error generating content from Gemini.")
				return
			}

			// Marshal the response to JSON
			respJSON, err := json.Marshal(resp)
			if err != nil {
				log.Println("Error marshaling response:", err)
				s.ChannelMessageSend(m.ChannelID, "Error processing response from Gemini.")
				return
			}

			// Create Kafka message with channel ID
			kafkaMsg := KafkaMessage{
				ChannelID: m.ChannelID,
				Content:   respJSON,
			}
			kafkaMsgJSON, err := json.Marshal(kafkaMsg)
			if err != nil {
				log.Println("Error marshaling Kafka message:", err)
				s.ChannelMessageSend(m.ChannelID, "Error preparing message for Kafka.")
				return
			}

			// Produce message to Kafka
			err = kafkaWriter.WriteMessages(context.Background(),
				kafka.Message{
					Value: kafkaMsgJSON,
				},
			)
			if err != nil {
				log.Println("Error writing to Kafka:", err)
				s.ChannelMessageSend(m.ChannelID, "Error sending message to Kafka.")
				return
			}
		}
	})

	sess.Identify.Intents = discordgo.IntentsAllWithoutPrivileged

	err = sess.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer sess.Close()

	fmt.Println("The Bot is online")

	go func(s *discordgo.Session) {
		for {
			msg, err := kafkaReader.ReadMessage(context.Background())
			if err != nil {
				log.Fatal(err)
			}

			// Unmarshal Kafka message
			var kafkaMsg KafkaMessage
			if err := json.Unmarshal(msg.Value, &kafkaMsg); err != nil {
				log.Println("Error unmarshaling Kafka message:", err)
				continue
			}

			// Extract the generated text
			type Content struct {
				Parts []string `json:"Parts"`
			}
			type Response struct {
				Candidates []struct {
					Content Content `json:"Content"`
				} `json:"Candidates"`
			}

			var response Response
			if err := json.Unmarshal(kafkaMsg.Content, &response); err != nil {
				log.Println("Error unmarshaling response:", err)
				continue
			}

			// Send the generated text to the Discord channel
			for _, candidate := range response.Candidates {
				for _, part := range candidate.Content.Parts {
					s.ChannelMessageSend(kafkaMsg.ChannelID, part)
				}
			}
		}
	}(sess)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	<-sc
}
