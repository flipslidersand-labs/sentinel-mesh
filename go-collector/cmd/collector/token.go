package main

import (
	"bufio"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// openTokenStore opens the BadgerDB store at dataDir for the token
// subcommands. BadgerDB takes an exclusive lock on its directory, so this
// fails if `sentinel-collector serve` is still running against the same
// --data-dir — the collector must be stopped first. That's a deliberate
// tradeoff for this project's scale (#183/#191, per ADR-005): a live admin
// RPC would need its own bootstrap-auth story, which isn't justified yet.
func openTokenStore(dataDir string) (*store.Store, error) {
	st, err := store.New(dataDir)
	if err != nil {
		return nil, fmt.Errorf("open store at %s (if sentinel-collector serve is running against this --data-dir, stop it first — BadgerDB requires exclusive access): %w", dataDir, err)
	}
	return st, nil
}

func tokenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "token",
		Short: "Manage per-agent gRPC authentication tokens",
		Long: `token manages per-agent gRPC authentication tokens (#183, per ADR-005:
docs/adr/ADR-005-agent-auth-strategy.md).

Tokens are stored in the same BadgerDB data directory as collector
events/alerts (--data-dir, shared with "serve"). Because BadgerDB takes an
exclusive lock on its directory, "sentinel-collector serve" must be stopped
before running any token subcommand against the same --data-dir.`,
	}
	cmd.PersistentFlags().String("data-dir", "/tmp/sentinel-data", "BadgerDB data directory (must match the collector's --data-dir)")
	cmd.AddCommand(tokenIssueCmd(), tokenRevokeCmd(), tokenListCmd())
	return cmd
}

func tokenIssueCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "issue <node_id>",
		Short: "Issue a new token for an agent",
		Long: `issue generates a new per-agent token and prints it once.

The token is never stored in plaintext — only its hash is persisted — so
this is the only time it's retrievable. Copy it into the agent's config
immediately (see scripts/deploy-agent.sh).

Fails if node_id already has an active (non-revoked) token; run
"token revoke <node_id>" first to reissue.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeID := strings.TrimSpace(args[0])
			if nodeID == "" {
				return fmt.Errorf("node_id must not be empty")
			}
			dataDir, _ := cmd.Flags().GetString("data-dir")
			st, err := openTokenStore(dataDir)
			if err != nil {
				return err
			}
			defer st.Close() //nolint:errcheck

			token, err := st.IssueToken(cmd.Context(), nodeID)
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), token)
			return nil
		},
	}
}

func tokenRevokeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revoke <node_id>",
		Short: "Revoke an agent's token",
		Long: `revoke marks node_id's current token as revoked. The agent immediately
loses the ability to authenticate; it must be issued a new token
("token issue <node_id>") and reconfigured to reconnect.

Prompts for confirmation unless --yes is given.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeID := strings.TrimSpace(args[0])
			if nodeID == "" {
				return fmt.Errorf("node_id must not be empty")
			}
			yes, _ := cmd.Flags().GetBool("yes")
			if !yes {
				fmt.Fprintf(cmd.OutOrStdout(), "Revoke token for node_id %q? [y/N] ", nodeID)
				line, _ := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
				if strings.ToLower(strings.TrimSpace(line)) != "y" {
					fmt.Fprintln(cmd.OutOrStdout(), "aborted")
					return nil
				}
			}
			dataDir, _ := cmd.Flags().GetString("data-dir")
			st, err := openTokenStore(dataDir)
			if err != nil {
				return err
			}
			defer st.Close() //nolint:errcheck

			return st.RevokeToken(cmd.Context(), nodeID)
		},
	}
	cmd.Flags().Bool("yes", false, "skip the confirmation prompt")
	return cmd
}

func tokenListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all issued tokens",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dataDir, _ := cmd.Flags().GetString("data-dir")
			st, err := openTokenStore(dataDir)
			if err != nil {
				return err
			}
			defer st.Close() //nolint:errcheck

			tokens, err := st.ListTokens(cmd.Context())
			if err != nil {
				return err
			}

			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
			fmt.Fprintln(w, "NODE_ID\tISSUED_AT\tSTATUS")
			for _, tok := range tokens {
				status := "active"
				if tok.RevokedAt != nil {
					status = "revoked"
				}
				fmt.Fprintf(w, "%s\t%s\t%s\n", tok.NodeID, tok.IssuedAt.Format(time.RFC3339), status)
			}
			return w.Flush()
		},
	}
}
