package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/mr-tron/base58"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	pbStore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	getServer      string
	getKey         string
	getBlockNumber uint64
	getOmitDeleted bool
)

// GetCmd represents the get command
var GetCmd = &cobra.Command{
	Use:   "get",
	Short: "Get a value from the foundational-store using gRPC",
	Long: `Get a value from the foundational-store using gRPC.
This command connects to a gRPC server and retrieves a value for the specified key.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if getKey == "" {
			return fmt.Errorf("key is required")
		}

		if getServer == "" {
			return fmt.Errorf("server address is required")
		}

		// Decode the key from base58
		keyBytes, err := base58.Decode(getKey)
		if err != nil {
			return fmt.Errorf("failed to decode key as base58: %w", err)
		}

		// Connect to the gRPC server
		fmt.Printf("Connecting to gRPC server at %s\n", getServer)
		conn, err := grpc.Dial(getServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to server: %w", err)
		}
		defer conn.Close()

		// Create a client for the StoreKV service
		client := pbStore.NewStoreKVClient(conn)

		// Create the GetRequest
		request := &pbStore.GetRequest{
			BlockNumber: getBlockNumber,
			OmitDeleted: getOmitDeleted,
			Key:         keyBytes,
		}

		// Make the Get request
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		fmt.Printf("Sending Get request for key: %s\n", getKey)
		start := time.Now()
		response, err := client.Get(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to get value: %w", err)
		}
		fmt.Printf("Query time: %s\n", time.Since(start))

		// Display the response
		switch response.Response {
		case pbStore.ResponseCode_FOUND:
			fmt.Println("Value found!")
			fmt.Printf("Type URL: %s\n", response.Value.TypeUrl)
			fmt.Printf("Value size: %d bytes\n", len(response.Value.Value))
		case pbStore.ResponseCode_NOT_FOUND:
			fmt.Println("Value not found")
		case pbStore.ResponseCode_NOT_FOUND_FINALIZE:
			fmt.Println("Value not found (finalized)")
		default:
			fmt.Printf("Unknown response code: %s\n", response.Response)
		}

		return nil
	},
}

func init() {
	GetCmd.Flags().StringVar(&getServer, "server", "localhost:50051", "gRPC server address")
	GetCmd.Flags().StringVar(&getKey, "key", "", "Key to lookup (base58 encoded)")
	GetCmd.Flags().Uint64Var(&getBlockNumber, "block-number", 0, "Block number for the query")
	GetCmd.Flags().BoolVar(&getOmitDeleted, "omit-deleted", false, "Whether to omit deleted values")

	GetCmd.MarkFlagRequired("key")
	//GetCmd.MarkFlagRequired("server")

	viper.BindPFlag("get.server", GetCmd.Flags().Lookup("server"))
	viper.BindPFlag("get.key", GetCmd.Flags().Lookup("key"))
	viper.BindPFlag("get.block_number", GetCmd.Flags().Lookup("block-number"))
	viper.BindPFlag("get.omit_deleted", GetCmd.Flags().Lookup("omit-deleted"))
}
