package protocols

const (
	// ClientSubscribeProtocol is used by a client to subscribe to a topic.
	// Client opens stream -> writes topic string -> server ACK/closes.
	ClientSubscribeProtocol = "/tatanka/subscribe/1.0.0"

	// ClientUnsubscribeProtocol is used by a client to unsubscribe from a topic.
	// Client opens stream -> writes topic string -> server ACK/closes.
	ClientUnsubscribeProtocol = "/tatanka/unsubscribe/1.0.0"

	// ClientSubscriptionsProtocol is used by a client to get the list of topics
	// they are subscribed to.
	ClientSubscriptionsProtocol = "/tatanka/subscriptions/1.0.0"

	// ClientPublishProtocol is used by a client to publish a message.
	// Client opens stream -> writes topic string -> writes data -> server closes.
	ClientPublishProtocol = "/tatanka/publish/1.0.0"

	// ClientPushProtocol is the persistent stream a client opens
	// to *receive* messages from the server.
	ClientPushProtocol = "/tatanka/push/1.0.0"

	// ClientRelayMessageProtocol is used by a client to send a message to another
	// client through the mesh and receive a response.
	ClientRelayMessageProtocol = "/tatanka/client-relay-message/1.0.0"

	// TatankaRelayMessageProtocol is used by a tatanka node to deliver a relay
	// message to a client on behalf of another client.
	TatankaRelayMessageProtocol = "/tatanka/tatanka-relay-message/1.0.0"

	// PostBondsProtocol is used by a client to share their bonds with the mesh.
	// This must be the first protocol to be opened after a client connects to a
	// tatanka node.
	PostBondsProtocol = "/tatanka/post-bonds/1.0.0"

	// AvailableMeshNodesProtocol is used by a client to get a list of all
	// tatanka nodes in the mesh that this node is connected to.
	AvailableMeshNodesProtocol = "/tatanka/available-mesh-nodes/1.0.0"

	// SearchTopicsProtocol is used by a client to search for topics in the mesh.
	SearchTopicsProtocol = "/tatanka/search-topics/1.0.0"
)

var (
	// PriceTopicPrefix is the prefix for price topics.
	PriceTopicPrefix = "price:"
	// FeeRateTopicPrefix is the prefix for fee rate topics.
	FeeRateTopicPrefix = "fee_rate:"
)
