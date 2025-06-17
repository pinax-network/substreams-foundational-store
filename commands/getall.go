package commands

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mr-tron/base58"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	pbStore "github.com/streamingfast/substreams-foundational-store/pb/sf/substreams/foundational-store/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GetAllCmd represents the getall command
var GetAllCmd = &cobra.Command{
	Use:   "getall",
	Short: "Get multiple values from the foundational-store using gRPC",
	Long: `Get multiple values from the foundational-store using gRPC.
This command connects to a gRPC server and retrieves values for the specified keys.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get flag values
		getAllKeys, _ := cmd.Flags().GetString("keys")
		getAllServer, _ := cmd.Flags().GetString("server")
		getAllBlockNumber, _ := cmd.Flags().GetUint64("block-number")
		getAllOmitDeleted, _ := cmd.Flags().GetBool("omit-deleted")

		if getAllKeys == "" {
			return fmt.Errorf("keys are required")
		}

		if getAllServer == "" {
			return fmt.Errorf("server address is required")
		}

		// Split the comma-separated keys
		keyStrings := strings.Split(getAllKeys, ",")
		if len(keyStrings) == 0 {
			return fmt.Errorf("at least one key is required")
		}

		// Decode the keys from base58
		keyBytes := make([][]byte, len(keyStrings))
		for i, keyString := range keyStrings {
			decoded, err := base58.Decode(strings.TrimSpace(keyString))
			if err != nil {
				return fmt.Errorf("failed to decode key %s as base58: %w", keyString, err)
			}
			keyBytes[i] = decoded
		}

		// Connect to the gRPC server
		fmt.Printf("Connecting to gRPC server at %s\n", getAllServer)
		conn, err := grpc.Dial(getAllServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to server: %w", err)
		}
		defer conn.Close()

		// Create a client for the StoreKV service
		client := pbStore.NewStoreKVClient(conn)

		// Create the GetAllRequest
		request := &pbStore.GetAllRequest{
			BlockNumber: getAllBlockNumber,
			OmitDeleted: getAllOmitDeleted,
			Keys:        keyBytes,
		}

		// Make the GetAll request
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		fmt.Printf("Sending GetAll request for %d keys\n", len(keyStrings))
		start := time.Now()
		response, err := client.GetAll(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to get values: %w", err)
		}
		fmt.Printf("Query time: %s\n", time.Since(start))

		// Display the response
		fmt.Printf("Received %d entries\n", len(response.Entries))
		for i, entry := range response.Entries {
			fmt.Printf("\nEntry %d:\n", i+1)
			fmt.Printf("  Key: %s\n", base58.Encode(entry.Key))

			switch entry.Response.Response {
			case pbStore.ResponseCode_FOUND:
				fmt.Println("  Status: Value found")
				fmt.Printf("  Type URL: %s\n", entry.Response.Value.TypeUrl)
				fmt.Printf("  Value size: %d bytes\n", len(entry.Response.Value.Value))
			case pbStore.ResponseCode_NOT_FOUND:
				fmt.Println("  Status: Value not found")
			case pbStore.ResponseCode_NOT_FOUND_FINALIZE:
				fmt.Println("  Status: Value not found (finalized)")
			default:
				fmt.Printf("  Status: Unknown response code: %s\n", entry.Response.Response)
			}
		}

		return nil
	},
}

func init() {
	GetAllCmd.Flags().String("server", "localhost:50051", "gRPC server address")
	GetAllCmd.Flags().String("keys", "", "Comma-separated list of keys to lookup (base58 encoded)")
	GetAllCmd.Flags().Uint64("block-number", 0, "Block number for the query")
	GetAllCmd.Flags().Bool("omit-deleted", false, "Whether to omit deleted values")

	GetAllCmd.MarkFlagRequired("keys")
	GetAllCmd.MarkFlagRequired("server")

	viper.BindPFlag("getall.server", GetAllCmd.Flags().Lookup("server"))
	viper.BindPFlag("getall.keys", GetAllCmd.Flags().Lookup("keys"))
	viper.BindPFlag("getall.block_number", GetAllCmd.Flags().Lookup("block-number"))
	viper.BindPFlag("getall.omit_deleted", GetAllCmd.Flags().Lookup("omit-deleted"))
}
