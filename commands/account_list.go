package commands

import (
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"os"
	"time"

	"github.com/mr-tron/base58"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Entry struct {
	BlockNumber uint64    `db:"block_number"`
	Key         string    `db:"key"`
	Value       []byte    `db:"value"`
	CreateTime  time.Time `db:"create_time"`
}

var (
	accountListFilePath   string
	accountListOutputPath string
)

// accountListCmd represents the account_list command
var AccountListCmd = &cobra.Command{
	Use:   "account-list",
	Short: "Extract accounts from a CSV file and save them to a binary file",
	Long: `Extract accounts from a CSV file containing account information and save them to a binary file using gob encoding.
This command is useful for preparing data for other commands like 'perf'.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Step 1: Open the file
		file, err := os.Open(accountListFilePath)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer file.Close()

		reader := csv.NewReader(file)

		// Step 2: Skip the header row
		_, err = reader.Read() // Read the first row, which contains column headers
		if err != nil {
			return fmt.Errorf("failed to read the CSV header: %w", err)
		}

		count := 0
		var accounts []string
		for {
			record, err := reader.Read()
			if err != nil {
				if err.Error() == "EOF" { // Detect end of file
					fmt.Println("Goodbye!")
					break
				}
				return fmt.Errorf("failed to read CSV record: %w", err)
			}

			if count > 0 && count%10000 == 0 {
				fmt.Print(".")
				_, err := base58.Decode(record[4])
				if err != nil {
					fmt.Println("Failed to decode base58:", record[4])
					continue
				}
				accounts = append(accounts, record[4])
			}
			if count > 0 && count%200000 == 0 {
				fmt.Println()
			}
			count += 1
		}

		fmt.Println()
		fmt.Println("Save to file")

		// Create a file for writing
		file, err = os.Create(accountListOutputPath)
		if err != nil {
			return fmt.Errorf("failed to create file: %w", err)
		}
		defer file.Close()

		// Create an encoder and send the accounts slice through it
		encoder := gob.NewEncoder(file)
		err = encoder.Encode(accounts)
		if err != nil {
			return fmt.Errorf("failed to encode accounts: %w", err)
		}

		fmt.Println("Successfully saved accounts to file")
		return nil
	},
}

func init() {
	AccountListCmd.Flags().StringVar(&accountListFilePath, "file", "/Users/cbillett/t/clickhouse-exports/small_initialized_accounts.csv", "Path to the CSV file")
	AccountListCmd.Flags().StringVar(&accountListOutputPath, "output", "/Users/cbillett/t/clickhouse-exports/small_account.bin", "Path to the output binary file")

	viper.BindPFlag("account_list.file", AccountListCmd.Flags().Lookup("file"))
	viper.BindPFlag("account_list.output", AccountListCmd.Flags().Lookup("output"))
}
