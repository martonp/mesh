package client

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bisoncraft/mesh/bond"
	tmc "github.com/bisoncraft/mesh/client"
	"github.com/bisoncraft/mesh/protocols"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	"github.com/decred/slog"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/libp2p/go-libp2p/core/crypto"
	"google.golang.org/protobuf/proto"
)

var (
	defaultTimeout = time.Second * 10

	DefaultWebPort    = 12465
	DefaultClientPort = 12455

	//go:embed index.html
	index embed.FS
)

// Config represents the test client configuration.
type Config struct {
	NodeAddr   []string
	PrivateKey crypto.PrivKey
	ClientPort int
	WebPort    int
	Logger     slog.Logger
}

// Client represents a tatanka test client.
type Client struct {
	cfg           *Config
	https         *http.Server
	tatankaClient *tmc.Client
	log           slog.Logger
	events        *eventStore
}

func (c *Client) route() {
	r := chi.NewRouter()
	r.Use(cors.AllowAll().Handler)

	r.Post("/broadcast", c.broadcast)
	r.Post("/subscribe", c.subscribe)
	r.Post("/unsubscribe", c.unsubscribe)
	r.Post("/bond/add", c.addBond)
	r.Get("/bond/postall", c.postAllBonds)
	r.Get("/identity", c.identity)
	r.Get("/subscriptions", c.subscriptions)
	r.Get("/events", c.getEvents)
	r.Get("/stream/events", c.streamEvents)

	// UI related endpoints.
	r.Get("/", c.serveUIIndexFromDist)
	r.Handle("/assets/*", http.HandlerFunc(c.serveUIDistAsset))
	r.Get("/*", c.serveUIIndexFromDist)

	c.https = &http.Server{
		Addr:    fmt.Sprintf(":%d", c.cfg.WebPort),
		Handler: r,
	}
}

func NewClient(cfg *Config) (*Client, error) {
	c := &Client{
		cfg:    cfg,
		log:    cfg.Logger,
		events: newEventStore(),
	}

	placeholderBond := &bond.BondParams{
		ID:       "placeholder",
		Expiry:   time.Now().Add(time.Hour * 6),
		Strength: bond.MinRequiredBondStrength,
	}

	tcCfg := &tmc.Config{
		Port:            cfg.ClientPort,
		PrivateKey:      cfg.PrivateKey,
		RemotePeerAddrs: []string(cfg.NodeAddr),
		Logger:          cfg.Logger,
		Bonds:           []*bond.BondParams{placeholderBond},
	}

	tc, err := tmc.NewClient(tcCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize tatanka client: %w", err)
	}

	c.tatankaClient = tc

	c.route()

	return c, nil
}

func (c *Client) identity(w http.ResponseWriter, r *http.Request) {
	peerID := c.tatankaClient.PeerID()
	tatankaNodePeerID := c.tatankaClient.ConnectedTatankaNodePeerID()
	resp := map[string]any{
		"peer_id":                "",
		"connected_node_peer_id": "",
	}
	if peerID != "" {
		resp["peer_id"] = peerID.String()
	}
	if tatankaNodePeerID != "" {
		resp["connected_node_peer_id"] = tatankaNodePeerID
	}
	if err := writeResponse(w, http.StatusOK, resp); err != nil {
		c.log.Errorf("Failed to write identity response: %v", err)
	}
}

func (c *Client) subscriptions(w http.ResponseWriter, r *http.Request) {
	topics := c.tatankaClient.Topics()
	if err := writeResponse(w, http.StatusOK, topics); err != nil {
		c.log.Errorf("Failed to write subscriptions response: %v", err)
	}
}

func (c Client) handleTopicEvent(topic string, evt tmc.TopicEvent) {
	switch evt.Type {
	case tmc.TopicEventData:
		// Store and stream typed event for the UI.
		c.events.add(Event{
			Type:    EventTypeData,
			Topic:   topic,
			Peer:    evt.Peer.String(),
			Message: decodeTopicData(topic, evt.Data),
		})
		c.log.Infof("topic=%s event=data peer=%s data=%s", topic, evt.Peer.String(), hex.Dump(evt.Data))
	case tmc.TopicEventPeerSubscribed:
		c.events.add(Event{
			Type:  EventTypePeerSubscribed,
			Topic: topic,
			Peer:  evt.Peer.String(),
		})
		c.log.Infof("topic=%s event=peer_subscribed peer=%s", topic, evt.Peer.String())
	case tmc.TopicEventPeerUnsubscribed:
		c.events.add(Event{
			Type:  EventTypePeerUnsubscribed,
			Topic: topic,
			Peer:  evt.Peer.String(),
		})
		c.log.Infof("topic=%s event=peer_unsubscribed peer=%s", topic, evt.Peer.String())
	default:
		c.log.Errorf("topic=%s event=unexpected type=%T", topic, evt.Type)
	}
}

func (c *Client) broadcast(w http.ResponseWriter, r *http.Request) {
	var payload broadcastPayload
	err := readRequest(r, &payload)
	if err != nil {
		c.log.Errorf("Failed to read broadcast request: %v", err)
		writeErrorResponse(w, http.StatusBadRequest, "failed to read broadcast request")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()

	data, err := base64.StdEncoding.DecodeString(payload.Data)
	if err != nil {
		c.log.Errorf("Failed to decode payload data: %v", err)
		writeErrorResponse(w, http.StatusBadRequest, "failed to decode payload data")
		return
	}

	err = c.tatankaClient.Broadcast(ctx, payload.Topic, data)
	if err != nil {
		c.log.Errorf("Failed to broadcast message: %v", err)
		writeErrorResponse(w, http.StatusInternalServerError, "failed to broadcast message")
		return
	}

	// Record outgoing broadcast so the UI can reload sent messages after refresh.
	c.events.add(Event{
		Type:    EventTypeBroadcast,
		Topic:   payload.Topic,
		Peer:    "self",
		Message: decodeTopicData(payload.Topic, data),
	})

	c.log.Infof("Broadcasted message on topic %s", payload.Topic)

	writeStatusResponse(w, http.StatusOK)
}

func (c *Client) subscribe(w http.ResponseWriter, r *http.Request) {
	var payload subscribePayload
	err := readRequest(r, &payload)
	if err != nil {
		c.log.Errorf("Failed to read subscribe request: %v", err)
		writeErrorResponse(w, http.StatusBadRequest, "failed to read subscribe request")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()

	err = c.tatankaClient.Subscribe(ctx, []string{payload.Topic}, func(_ string, evt tmc.TopicEvent) {
		c.handleTopicEvent(payload.Topic, evt)
	})
	if err != nil {
		c.log.Errorf("Failed to subscribe to topic %s: %v", payload.Topic, err)
		writeErrorResponse(w, http.StatusInternalServerError, "failed to subscribe to topic")
		return
	}

	c.events.add(Event{
		Type:  EventTypeSubscribed,
		Topic: payload.Topic,
		Peer:  "self",
	})
	c.log.Infof("Subscribed to topic %s", payload.Topic)

	writeStatusResponse(w, http.StatusOK)
}

func (c *Client) unsubscribe(w http.ResponseWriter, r *http.Request) {
	var payload subscribePayload
	err := readRequest(r, &payload)
	if err != nil {
		c.log.Errorf("Failed to read unsubscribe request: %v", err)
		writeErrorResponse(w, http.StatusBadRequest, "failed to read unsubscribe request")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()

	err = c.tatankaClient.Unsubscribe(ctx, payload.Topic)
	if err != nil {
		c.log.Errorf("Failed to unsubscribe from topic %s: %v", payload.Topic, err)
		writeErrorResponse(w, http.StatusInternalServerError, "failed to unsubscribe from topic")
		return
	}

	c.events.add(Event{
		Type:  EventTypeUnsubscribed,
		Topic: payload.Topic,
		Peer:  "self",
	})
	c.log.Infof("Unsubscribed from topic %s", payload.Topic)

	writeStatusResponse(w, http.StatusOK)
}

func (c *Client) addBond(w http.ResponseWriter, r *http.Request) {
	var payload bondPayload
	err := readRequest(r, &payload)
	if err != nil {
		c.log.Errorf("Failed to read bond request: %v", err)
		writeErrorResponse(w, http.StatusBadRequest, "failed to read add bond request")
		return
	}

	bonds := []*bond.BondParams{{
		ID:       payload.ID,
		Expiry:   time.Unix(payload.Expiry, 0),
		Strength: payload.Strength,
	}}

	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()

	err = c.tatankaClient.AddBonds(ctx, bonds)
	if err != nil {
		c.log.Errorf("Failed to add bond: %v", err)
		writeErrorResponse(w, http.StatusInternalServerError, "failed to add bond")
		return
	}

	c.log.Infof("Added bond %s with strength %d", payload.ID, payload.Strength)

	writeStatusResponse(w, http.StatusOK)
}

func (c *Client) postAllBonds(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), defaultTimeout)
	defer cancel()

	err := c.tatankaClient.PostAllBonds(ctx)
	if err != nil {
		c.log.Errorf("Failed to post bond: %v", err)
		writeErrorResponse(w, http.StatusInternalServerError, "failed to post bond")
		return
	}

	writeStatusResponse(w, http.StatusOK)
}

func (c *Client) getEvents(w http.ResponseWriter, r *http.Request) {
	afterID := parseAfterID(r)
	events := c.events.listSince(afterID)
	if err := writeResponse(w, http.StatusOK, events); err != nil {
		c.log.Errorf("Failed to write events response: %v", err)
	}
}

func parseAfterID(r *http.Request) uint64 {
	if v := r.URL.Query().Get("after"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			return n
		}
	}
	return 0
}

func (c *Client) streamEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("Connection", "keep-alive")

	rc := http.NewResponseController(w)
	_, err := fmt.Fprint(w, "data: connected\n\n")
	if err != nil {
		c.log.Errorf("Failed to write to stream: %v", err)
		return
	}
	rc.Flush()

	if _, err := fmt.Fprint(w, "data: connected\n\n"); err != nil {
		return
	}
	rc.Flush()

	// Determine replay point.
	var afterID uint64
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		if n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64); err == nil {
			afterID = n
		}
	}
	if afterID == 0 {
		afterID = parseAfterID(r)
	}

	// Replay stored events first.
	for _, e := range c.events.listSince(afterID) {
		if err := writeSSEEvent(w, e); err != nil {
			return
		}
		rc.Flush()
		afterID = e.ID
	}

	// Then stream live events.
	ch, cancel := c.events.subscribe()
	defer cancel()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return
			}
			if e.ID <= afterID {
				continue
			}
			if err := writeSSEEvent(w, e); err != nil {
				return
			}
			rc.Flush()
			afterID = e.ID

		case <-ticker.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			rc.Flush()

		case <-r.Context().Done():
			return
		}
	}
}

func writeSSEEvent(w http.ResponseWriter, e Event) error {
	js, err := encodeEventJSON(e)
	if err != nil {
		return err
	}
	// SSE format: include id so the browser can reconnect with Last-Event-ID.
	_, err = fmt.Fprintf(w, "id: %d\nevent: tatanka\ndata: %s\n\n", e.ID, js)
	return err
}

func (c *Client) Run(ctx context.Context) {
	c.log.Infof("Running test client ...")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		c.log.Infof("Tatanka client listening on: %d", c.cfg.ClientPort)
		err := c.tatankaClient.Run(ctx)
		if err != nil {
			c.log.Errorf("Failed to run tatanka client: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_ = c.https.Shutdown(ctx)
	}()

	go func() {
		defer wg.Done()
		c.log.Infof("Web interface available on: %d", c.cfg.WebPort)
		if err := c.https.ListenAndServe(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				c.log.Errorf("Failed to start server: %v", err)
				return
			}
		}
	}()

	wg.Wait()
}

// decodeTopicData decodes topic data to a human-readable string.
func decodeTopicData(topic string, data []byte) string {
	if strings.HasPrefix(topic, protocols.PriceTopicPrefix) {
		ticker := topic[len(protocols.PriceTopicPrefix):]
		var priceUpdate protocolsPb.ClientPriceUpdate
		if err := proto.Unmarshal(data, &priceUpdate); err == nil {
			return fmt.Sprintf("%s: $%.2f", ticker, priceUpdate.Prices[ticker])
		}
	} else if strings.HasPrefix(topic, protocols.FeeRateTopicPrefix) {
		network := topic[len(protocols.FeeRateTopicPrefix):]
		var feeRateUpdate protocolsPb.ClientFeeRateUpdate
		if err := proto.Unmarshal(data, &feeRateUpdate); err == nil {
			feeRate := new(big.Int).SetBytes(feeRateUpdate.FeeRate)
			return fmt.Sprintf("%s: %s", network, feeRate.String())
		}
	}
	// Default: treat as UTF-8 text
	return string(data)
}
