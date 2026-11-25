package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/flipslidersand/sentinel-mesh/internal/receiver"
	"github.com/flipslidersand/sentinel-mesh/internal/store"
)

// openTokenStore opens the BadgerDB store at dataDir for the token
// subcommands. BadgerDB takes an exclusive lock on its directory, so this
// fails if `sentinel-collector serve` is still running against the same
// --data-dir. issue, list and a plain `token revoke` are delegated to serve's
// local admin socket first (#572, #574, see admin.go) and only reach this when
// no serve is listening; revoke --force still needs serve stopped, a
// deliberate tradeoff for this project's scale (#183/#191, per ADR-005).
func openTokenStore(dataDir string) (*store.Store, error) {
	st, err := store.New(dataDir)
	if err != nil {
		return nil, fmt.Errorf("open store at %s (if sentinel-collector serve is running against this --data-dir, `token issue`, `token list` and `token revoke <node_id>` work without stopping it unless its admin socket could not be opened; stop it first for revoke --force — BadgerDB requires exclusive access): %w", dataDir, err)
	}
	return st, nil
}

const (
	flagDataDir = "data-dir"
	flagYes     = "yes"
	flagForce   = "force"
)

// tokenDataDir reads --data-dir, returning the lookup error instead of
// silently yielding an empty path.
func tokenDataDir(cmd *cobra.Command) (string, error) {
	r := &flagReader{cmd: cmd}
	dir := r.str(flagDataDir)
	return dir, r.err
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
before running "token revoke --force" against the same --data-dir. "token
issue", "token list" and a plain "token revoke <node_id>" also work while
serve is running: they are sent to serve over a local Unix socket
(<data-dir>/admin/admin.sock, directory mode 0700). If serve could not open
that socket (it logs a warning), stop serve for these too.`,
	}
	cmd.PersistentFlags().String(flagDataDir, "/tmp/sentinel-data", "BadgerDB data directory (must match the collector's --data-dir)")
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

If "serve" is running against the same --data-dir, the token is issued by it
over the local admin socket (<data-dir>/admin/admin.sock) and is usable by the
agent at once; the plaintext travels only over that socket and is not logged.
If the admin socket directory exists but is not a real directory owned by you
with mode 0700, issue fails instead of falling back to the direct path.

Fails if node_id already has an active (non-revoked) token; run
"token revoke <node_id>" first to reissue.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeID := strings.TrimSpace(args[0])
			if nodeID == "" {
				return fmt.Errorf("node_id must not be empty")
			}
			if err := receiver.ValidateNodeID(nodeID); err != nil {
				return err
			}
			dataDir, err := tokenDataDir(cmd)
			if err != nil {
				return err
			}
			token, delegated, err := issueViaAdmin(cmd.Context(), dataDir, nodeID)
			if err != nil {
				return err
			}
			if !delegated {
				st, err := openTokenStore(dataDir)
				if err != nil {
					return err
				}
				defer st.Close() //nolint:errcheck

				token, err = st.IssueToken(cmd.Context(), nodeID)
				if err != nil {
					return err
				}
			}
			fmt.Fprintln(cmd.OutOrStdout(), token)
			return nil
		},
	}
}

func tokenRevokeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revoke <node_id | --force record_key>",
		Short: "Revoke an agent's token",
		Long: `revoke marks node_id's current token as revoked. If "serve" is running
against the same --data-dir, the revoke is delegated to it over the local
admin socket (<data-dir>/admin/admin.sock): the agent can no longer
authenticate, and its already open streams are closed within
heartbeat-timeout/3. If serve is stopped, the token store is opened directly
and the revoke takes effect when the collector is started again. Either way
the agent must be issued a new token ("token issue <node_id>") and
reconfigured to reconnect.

If the admin socket directory exists but is not a real directory owned by you
with mode 0700, revoke fails instead of falling back to the direct path.

With --force, the argument is instead the token record key (as printed in
the "token list" warning for a record that could not be decoded, e.g.
agenttoken:<node_id>). The record is deleted without being decoded; this is
the way to remove a damaged record that plain revoke cannot read. It fails if
the key does not exist. --force is never delegated: it needs serve stopped.

Prompts for confirmation unless --yes is given.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeID := strings.TrimSpace(args[0])
			if nodeID == "" {
				return fmt.Errorf("node_id must not be empty")
			}
			r := &flagReader{cmd: cmd}
			yes := r.boolean(flagYes)
			force := r.boolean(flagForce)
			dataDir := r.str(flagDataDir)
			if r.err != nil {
				return r.err
			}
			if !yes {
				if force {
					fmt.Fprintf(cmd.OutOrStdout(), "Delete token record %q without decoding it? [y/N] ", nodeID)
				} else {
					fmt.Fprintf(cmd.OutOrStdout(), "Revoke token for node_id %q? [y/N] ", nodeID)
				}
				line, _ := bufio.NewReader(cmd.InOrStdin()).ReadString('\n')
				if strings.ToLower(strings.TrimSpace(line)) != "y" {
					fmt.Fprintln(cmd.OutOrStdout(), "aborted")
					return nil
				}
			}
			if !force {
				result, delegated, err := revokeViaAdmin(cmd.Context(), dataDir, nodeID)
				if err != nil {
					return err
				}
				if delegated {
					// Same silence as the direct path for an unknown or
					// already revoked node_id.
					if result == adminResultRevoked {
						fmt.Fprintln(cmd.OutOrStdout(), "revoked on the running collector; existing streams close within heartbeat-timeout/3")
					}
					return nil
				}
			}
			st, err := openTokenStore(dataDir)
			if err != nil {
				return err
			}
			defer st.Close() //nolint:errcheck

			if force {
				return st.RevokeRawRecord(cmd.Context(), nodeID)
			}
			return st.RevokeToken(cmd.Context(), nodeID)
		},
	}
	cmd.Flags().Bool(flagYes, false, "skip the confirmation prompt")
	cmd.Flags().Bool(flagForce, false, "treat the argument as a token record key and delete that record without decoding it")
	return cmd
}

func tokenListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all issued tokens",
		Long: `list prints every token record (revoked and active). If "serve" is running
against the same --data-dir, the list is read from it over the local admin
socket (<data-dir>/admin/admin.sock); the output is the same either way. A record
that cannot be decoded is skipped with a warning, which is written to the
collector's log (not the CLI's stderr) when the list is delegated.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dataDir, err := tokenDataDir(cmd)
			if err != nil {
				return err
			}
			tokens, delegated, err := listViaAdmin(cmd.Context(), dataDir)
			if err != nil {
				return err
			}
			if !delegated {
				st, err := openTokenStore(dataDir)
				if err != nil {
					return err
				}
				defer st.Close() //nolint:errcheck

				tokens, err = st.ListTokens(cmd.Context())
				if err != nil {
					return err
				}
			}
			return printTokenList(cmd.OutOrStdout(), tokens)
		},
	}
}

// printTokenList renders the table shared by the direct and delegated paths.
func printTokenList(out io.Writer, tokens []store.AgentToken) error {
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NODE_ID\tISSUED_AT\tSTATUS")
	for _, tok := range tokens {
		status := "active"
		if tok.RevokedAt != nil {
			status = "revoked"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\n", tok.NodeID, tok.IssuedAt.Format(time.RFC3339), status)
	}
	return w.Flush()
}
