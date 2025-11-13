package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	gmail "google.golang.org/api/gmail/v1"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

type stringSlice []string

func (s *stringSlice) String() string { return strings.Join(*s, ",") }
func (s *stringSlice) Set(val string) error {
	*s = append(*s, val)
	return nil
}

var (
	dryRun        bool
	verbose       bool
	startPath     string
	excludes      stringSlice
	probeDst      bool
	probeName     string
	probeContains bool
	rootOnly      bool
	noRateLimit   bool
	maxQPS        int
	emailEnabled  bool
	emailInterval time.Duration
)

// stats collects post-run summary information
var stats = struct {
	FilesTotal         int
	FilesPlanned       int
	FilesCopied        int
	FilesFailed        int
	FilesSkippedNative int
	BytesCopied        int64
	Start              time.Time
	End                time.Time
}{}

// Interval for periodic status logs
var statusLogInterval = 5 * time.Minute

// rate limiting globals
var (
	projectLimiter *rate.Limiter
	userLimiters   = map[string]*rate.Limiter{}
	userLimitersMu sync.Mutex
)

// track current source folder name for status emails
var (
	currentFolder   string
	currentFolderMu sync.Mutex
)

// email thread management for status emails
var (
	emailThreadID  string
	emailThreadMu  sync.Mutex
	emailMessageID string
	emailMessageMu sync.Mutex
)

// default limits (tunable at runtime)
var (
	defaultProjectQPS   = 10
	defaultProjectBurst = 20
	defaultUserQPS      = 3
	defaultUserBurst    = 6
)

var gormDB *gorm.DB

// FileRecord is the GORM model for the checkpoint DB
type FileRecord struct {
	SrcID     string    `gorm:"primaryKey;column:src_id"`
	SrcName   string    `gorm:"column:src_name"`
	DstID     string    `gorm:"column:dst_id"`
	Status    string    `gorm:"column:status"`
	Size      int64     `gorm:"column:size"`
	Retries   int       `gorm:"column:retries"`
	LastError string    `gorm:"column:last_error"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
}

func init() {
	flag.BoolVar(&dryRun, "dry-run", false, "Print actions without making writes")
	flag.BoolVar(&verbose, "v", false, "Verbose logging")
	flag.StringVar(&startPath, "start-path", "", "Start path under My Drive (e.g. 'USB Stuff.../Photo Stuff')")
	flag.Var(&excludes, "exclude", "Folder name to exclude (repeatable). Exact match. e.g. -exclude 'JPG Images' -exclude 'RAW Images'")
	flag.BoolVar(&probeDst, "probe-dst", false, "Probe destination Drive for an item by name and exit")
	flag.StringVar(&probeName, "probe-name", "", "Name to probe for when -probe-dst is used")
	flag.BoolVar(&probeContains, "probe-contains", false, "When probing, match names that contain the probe-name (substring) instead of exact match")
	flag.BoolVar(&rootOnly, "root-only", false, "Copy/migrate files directly under My Drive root only (no folders)")
	flag.BoolVar(&noRateLimit, "no-rate-limit", false, "Disable internal rate limiter waits (use with care)")
	flag.IntVar(&maxQPS, "max-qps", defaultProjectQPS, "Max queries per second for project-level limiter (overrides default)")
	flag.BoolVar(&emailEnabled, "status-email", false, "Enable periodic status emails sent from destination account to source account")
	flag.DurationVar(&emailInterval, "status-email-interval", 30*time.Minute, "Interval between status emails when -status-email is enabled")
}

func main() {
	flag.Parse()

	// prepare logs
	if err := os.MkdirAll("logs", 0755); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logs dir: %v\n", err)
		os.Exit(1)
	}

	logFile := filepath.Join("logs", fmt.Sprintf("gdrive-migrate-%s.log", time.Now().Format("20060102-150405")))
	f, ferr := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

	if ferr != nil {
		fmt.Fprintf(os.Stderr, "failed to open log file: %v\n", ferr)
		os.Exit(1)
	}
	defer f.Close()

	mw := io.MultiWriter(os.Stdout, f)
	logger := log.New(mw, "", log.LstdFlags)
	if verbose {
		logger.Println("verbose logging enabled")
	}

	ctx := context.Background()
	stats.Start = time.Now()

	// Open or create checkpoint DB
	if err := os.MkdirAll("state", 0750); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create state dir: %v\n", err)
		os.Exit(1)
	}
	dbPath := filepath.Join("state", "migrate.db")
	var dbErr error
	gormDB, dbErr = gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if dbErr != nil {
		logger.Fatalf("failed to open checkpoint DB via GORM: %v", dbErr)
	}
	// automigrate the files table
	if err := gormDB.AutoMigrate(&FileRecord{}); err != nil {
		logger.Fatalf("failed to auto-migrate DB: %v", err)
	}

	// initialize rate limiters (can be adjusted later)
	rand.Seed(time.Now().UnixNano())
	// allow overriding project QPS via -max-qps flag; fall back to default if invalid
	if maxQPS <= 0 {
		maxQPS = defaultProjectQPS
	}
	projectLimiter = rate.NewLimiter(rate.Limit(maxQPS), defaultProjectBurst)

	// Prepare secure credentials directory and build drive services from there
	credsDir := filepath.Join("state", "creds")
	if err := ensureSecureCredsDir(credsDir, logger); err != nil {
		logger.Fatalf("failed to prepare secure creds dir: %v", err)
	}
	srcService, err := buildDriveService(ctx, filepath.Join(credsDir, "creds_src_oauth.json"), filepath.Join(credsDir, "creds_src.json"), filepath.Join(credsDir, "src_token.json"))
	if err != nil {
		logger.Fatalf("failed to create source drive service: %v", err)
	}
	// Build destination services. If status-email is enabled, prefer a single OAuth
	// flow that requests both Drive and Gmail scopes so the saved token contains
	// permissions for both APIs. Otherwise, build a Drive-only service.
	var dstService *drive.Service
	var dstGmail *gmail.Service
	if emailEnabled {
		// build both Drive and Gmail services using a single OAuth consent (Drive + Gmail send)
		dstDrive, dstGm, derr := buildServicesFromOAuth(ctx, filepath.Join(credsDir, "creds_dst_oauth.json"), filepath.Join(credsDir, "creds_dst.json"), filepath.Join(credsDir, "dst_token.json"), drive.DriveScope, gmail.MailGoogleComScope)
		if derr != nil {
			logger.Fatalf("failed to create destination drive+gmail services: %v", derr)
		}
		dstService = dstDrive
		dstGmail = dstGm
	} else {
		dstService, err = buildDriveService(ctx, filepath.Join(credsDir, "creds_dst_oauth.json"), filepath.Join(credsDir, "creds_dst.json"), filepath.Join(credsDir, "dst_token.json"))
		if err != nil {
			logger.Fatalf("failed to create destination drive service: %v", err)
		}
	}

	// Probe mode
	if probeDst {
		if probeName == "" {
			logger.Fatalf("-probe-dst requires -probe-name to be set")
		}
		probeDestination(ctx, dstService, probeName, probeContains, logger)
		return
	}

	// Find start folder on source (or use root). Allow the user to paste a Drive folder link into -start-path.
	var startFolderID string
	if startPath == "" {
		startFolderID = "root"
	} else {
		// If startPath looks like a Drive link, extract folder ID
		if id := parseDriveFolderLink(startPath); id != "" {
			// we have a folder ID — resolve its path for logging and use the ID directly
			resolvedPath, rerr := resolveFolderIDToPath(ctx, srcService, id)
			if rerr != nil {
				logger.Fatalf("failed to resolve folder ID %s to path: %v", id, rerr)
			}

			if resolvedPath != "" {
				logger.Printf("resolved folder link to start-path: %s", resolvedPath)
				startPath = resolvedPath
			}

			startFolderID = id
		} else {
			startFolderID, err = resolvePathToFolderID(ctx, srcService, startPath)
			if err != nil {
				logger.Fatalf("failed to resolve start path %q: %v", startPath, err)
			}

			if startFolderID == "" {
				logger.Fatalf("start path %q not found", startPath)
			}
		}
	}

	// Determine destination account email (used for temporary permission grants)
	dstEmail := getAccountEmail(dstService)
	logger.Printf("destination account email: %s", dstEmail)

	// If email sending is enabled, start a background goroutine that emails periodic status from dst->src
	if emailEnabled {
		srcEmail := getAccountEmail(srcService)
		if srcEmail == "" {
			logger.Printf("warning: could not determine source account email; status emails will not be sent")
		} else if dstEmail == "" {
			logger.Printf("warning: could not determine destination account email; status emails will not be sent")
		} else {
			logger.Printf("status-email enabled: sending updates from %s to %s every %s", dstEmail, srcEmail, emailInterval)
			go func() {
				ticker := time.NewTicker(emailInterval)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						_ = sendProgressEmail(ctx, dstGmail, dstEmail, srcEmail, logger)
					}
				}
			}()
		}
	}

	// Estimate total bytes/files to process (quick sample) and start background estimator
	if verbose {
		logger.Println("performing a quick sample estimate; a full estimator will run in background")
	}

	// start background estimator which updates totalBytesToCopy periodically
	go startStatusLogger(ctx, logger, statusLogInterval)

	// Begin walking and migrating
	logger.Printf("starting migration from source folder ID: %s (startPath=%q)\n", startFolderID, startPath)

	if rootOnly {
		migrateRootFiles(ctx, srcService, dstService, dstEmail, logger)
	} else {
		// If starting at root, pre-share top-level folders with destination (user requested)
		if startFolderID == "root" {
			// list top-level folders
			topQ := "'root' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
			var topRes *drive.FileList
			err := withRateLimit(ctx, "", logger, func() error {
				var e error
				topRes, e = srcService.Files.List().Q(topQ).Fields("files(id,name)").Do()
				return e
			})
			if err != nil {
				logger.Fatalf("failed to list top-level folders: %v", err)
			}
			for _, tf := range topRes.Files {
				// create temporary reader permission on the folder for dstEmail
				perm := &drive.Permission{Type: "user", Role: "reader", EmailAddress: dstEmail}
				var pc *drive.Permission

				perr := withRateLimit(ctx, "", logger, func() error {
					var e error
					pc, e = srcService.Permissions.Create(tf.Id, perm).Do()
					return e
				})

				if perr != nil {
					logger.Printf("warning: failed to create temp permission on folder %s (%s): %v", tf.Name, tf.Id, perr)
				} else {
					logger.Printf("created temp permission %s on folder %s (%s)", pc.Id, tf.Name, tf.Id)
				}

				// migrate folder (will use server-side copy when possible)
				migrateFolderRecursive(ctx, srcService, dstService, tf.Id, "root", dstEmail, logger, "root")

				// revoke permission if created
				if pc != nil && pc.Id != "" {
					_ = withRateLimit(ctx, "", logger, func() error {
						return srcService.Permissions.Delete(tf.Id, pc.Id).Do()
					})
				}
			}

			// also migrate root files
			migrateRootFiles(ctx, srcService, dstService, dstEmail, logger)
		} else {
			// preserve files structure of ancestors of start folder
			dstParent := "root"
			parentPath := "root"
			if startPath != "" {
				parts := strings.Split(startPath, "/")
				if len(parts) > 1 {
					for _, p := range parts[:len(parts)-1] {
						p = strings.TrimSpace(p)
						if p == "" {
							continue
						}
						dstParent = dstFindOrCreateFolder(ctx, dstService, p, dstParent, logger)
						if parentPath == "root" {
							parentPath = "root/" + p
						} else {
							parentPath = parentPath + "/" + p
						}
					}
				}
			}

			migrateFolderRecursive(ctx, srcService, dstService, startFolderID, dstParent, dstEmail, logger, parentPath)
		}
	}

	stats.End = time.Now()
	logger.Println("migration finished")

	elapsed := stats.End.Sub(stats.Start)
	logger.Println("--- Summary ---")
	logger.Printf("files encountered: %d", stats.FilesTotal)
	if dryRun {
		logger.Printf("files planned (dry-run): %d", stats.FilesPlanned)
	}
	logger.Printf("files copied: %d", stats.FilesCopied)
	logger.Printf("files skipped (native Google formats): %d", stats.FilesSkippedNative)
	logger.Printf("files failed: %d", stats.FilesFailed)
	logger.Printf("bytes copied (approx): %s", formatBytes(stats.BytesCopied))
	logger.Printf("time elapsed: %s", elapsed)

	if emailEnabled && dstGmail != nil {
		srcEmailFinal := getAccountEmail(srcService)
		if srcEmailFinal == "" || dstEmail == "" {
			logger.Printf("warning: could not determine emails for final status email (src=%s dst=%s)", srcEmailFinal, dstEmail)
		} else {
			_ = sendFinalEmail(ctx, dstGmail, dstEmail, srcEmailFinal, logger)
		}
	}
}

func buildDriveService(ctx context.Context, oauthPath, credsPath, tokenPath string) (*drive.Service, error) {
	// Prefer oauth client secret if exists
	if _, err := os.Stat(oauthPath); err == nil {
		b, err := os.ReadFile(oauthPath)
		if err != nil {
			return nil, err
		}
		config, err := google.ConfigFromJSON(b, drive.DriveScope)
		if err != nil {
			return nil, err
		}
		client, err := getClient(config, tokenPath)
		if err != nil {
			return nil, err
		}
		return drive.NewService(ctx, option.WithHTTPClient(client))
	}

	// Fallback to service account / credentials JSON
	if _, err := os.Stat(credsPath); err == nil {
		// If it's a service account JSON, use it directly with option.WithCredentialsFile
		return drive.NewService(ctx, option.WithCredentialsFile(credsPath), option.WithScopes(drive.DriveScope))
	}

	return nil, fmt.Errorf("no suitable credentials found (tried %s and %s)", oauthPath, credsPath)
}

// buildGmailService mirrors buildDriveService but returns a Gmail API client using the same credential/token paths.
func buildGmailService(ctx context.Context, oauthPath, credsPath, tokenPath string) (*gmail.Service, error) {
	if _, err := os.Stat(oauthPath); err == nil {
		b, err := os.ReadFile(oauthPath)
		if err != nil {
			return nil, err
		}
		config, err := google.ConfigFromJSON(b, gmail.GmailSendScope)
		if err != nil {
			return nil, err
		}
		client, err := getClient(config, tokenPath)
		if err != nil {
			return nil, err
		}
		return gmail.NewService(ctx, option.WithHTTPClient(client))
	}

	if _, err := os.Stat(credsPath); err == nil {
		return gmail.NewService(ctx, option.WithCredentialsFile(credsPath), option.WithScopes(gmail.GmailSendScope))
	}

	return nil, fmt.Errorf("no suitable credentials found for Gmail (tried %s and %s)", oauthPath, credsPath)
}

// buildServicesFromOAuth constructs both Drive and Gmail clients using a single
// OAuth client (so the saved token contains all requested scopes). It will
// prefer a user OAuth client secret if present; otherwise it will fall back
// to creating both services from a credentials file when possible.
func buildServicesFromOAuth(ctx context.Context, oauthPath, credsPath, tokenPath string, scopes ...string) (*drive.Service, *gmail.Service, error) {
	if _, err := os.Stat(oauthPath); err == nil {
		b, err := os.ReadFile(oauthPath)
		if err != nil {
			return nil, nil, err
		}
		config, err := google.ConfigFromJSON(b, scopes...)
		if err != nil {
			return nil, nil, err
		}
		// detect whether a token file already exists so we can avoid double-prompting
		tokenExisted := false
		if _, terr := os.Stat(tokenPath); terr == nil {
			tokenExisted = true
		}

		client, err := getClient(config, tokenPath)
		if err != nil {
			return nil, nil, err
		}

		// Create services using the obtained client
		ds, err := drive.NewService(ctx, option.WithHTTPClient(client))
		if err != nil {
			return nil, nil, err
		}
		gs, err := gmail.NewService(ctx, option.WithHTTPClient(client))
		if err != nil {
			return ds, nil, err
		}

		// If a token already existed but lacked Gmail scopes, some Gmail calls will
		// fail with insufficient authentication scopes. this is a lightweight call to
		// detect that situation and, if detected, force a re-auth to obtain the
		// combined scopes by removing the token file and prompting again.
		// We only auto-force re-auth when a token existed before this function was
		// called; if we just created a token (tokenExisted == false) and the test
		// still fails, return an error instead of prompting again to avoid
		// double-prompting the user. Complicated but whatever
		if tokenExisted {
			if _, gerr := gs.Users.Labels.List("me").Do(); gerr != nil {
				if ge, ok := gerr.(*googleapi.Error); ok && ge.Code == 403 {
					// just fail, it's fine
					return nil, nil, fmt.Errorf("existing token %s appears to lack Gmail send scope; please re-authenticate interactively to grant Drive+Gmail scopes (remove the token file to trigger re-auth): initial gmail error: %v", tokenPath, gerr)
				}
			}
		} else {
			// token did not exist before; we just obtained it via interactive flow.
			// If a subsequent Gmail test fails with insufficient scopes, return
			// an informative error rather than prompting again.
			if _, gerr := gs.Users.Labels.List("me").Do(); gerr != nil {
				if ge, ok := gerr.(*googleapi.Error); ok && ge.Code == 403 {
					return nil, nil, fmt.Errorf("obtained token does not have Gmail send scope; ensure your OAuth client and consent screen allow Gmail scopes: %v", gerr)
				}
			}
		}

		return ds, gs, nil
	}

	// Fallback to credentials JSON (service account or other).
	if _, err := os.Stat(credsPath); err == nil {
		ds, derr := drive.NewService(ctx, option.WithCredentialsFile(credsPath), option.WithScopes(drive.DriveScope))
		if derr != nil {
			return nil, nil, derr
		}
		gs, gerr := gmail.NewService(ctx, option.WithCredentialsFile(credsPath), option.WithScopes(gmail.GmailSendScope))
		if gerr != nil {
			return ds, nil, gerr
		}
		return ds, gs, nil
	}

	return nil, nil, fmt.Errorf("no suitable credentials found (tried %s and %s)", oauthPath, credsPath)
}

func dbGetStatus(srcID string) (status string, dstID string, size int64, err error) {
	var rec FileRecord
	res := gormDB.First(&rec, "src_id = ?", srcID)
	if res.Error != nil {
		if res.Error == gorm.ErrRecordNotFound {
			return "", "", 0, nil
		}
		return "", "", 0, res.Error
	}
	return rec.Status, rec.DstID, rec.Size, nil
}

func dbMarkInProgress(srcID, name string, size int64) error {
	rec := FileRecord{SrcID: srcID, SrcName: name, Status: "in_progress", Size: size, UpdatedAt: time.Now()}
	return gormDB.Clauses().Save(&rec).Error
}

func dbMarkDone(srcID, dstID string, size int64) error {
	rec := FileRecord{SrcID: srcID}
	updates := map[string]interface{}{"dst_id": dstID, "status": "done", "size": size, "updated_at": time.Now()}
	return gormDB.Model(&rec).Where("src_id = ?", srcID).Updates(updates).Error
}

func dbMarkFailed(srcID string, errMsg string) error {
	rec := FileRecord{SrcID: srcID}
	updates := map[string]interface{}{"status": "failed", "last_error": errMsg, "updated_at": time.Now()}
	return gormDB.Model(&rec).Where("src_id = ?", srcID).Updates(updates).Error
}

func getAccountEmail(svc *drive.Service) string {
	about, err := svc.About.Get().Fields("user(emailAddress)").Do()
	if err != nil {
		return ""
	}
	if about.User != nil {
		return about.User.EmailAddress
	}
	return ""
}

// formatBytes converts bytes to a human-readable string with appropriate units (KB, MB, GB, TB, PB).
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	units := []string{"KB", "MB", "GB", "TB", "PB"}
	return fmt.Sprintf("%.2f %s", float64(bytes)/float64(div), units[exp])
}

func getClient(config *oauth2.Config, tokenFile string) (*http.Client, error) {
	ctx := context.Background()
	tok, err := tokenFromFile(tokenFile)
	if err != nil {

		authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
		fmt.Printf("Go to the following link in your browser then type the authorization code:\n%v\n", authURL)
		var code string
		if _, err := fmt.Scan(&code); err != nil {
			return nil, err
		}

		tok, err = config.Exchange(ctx, code)
		if err != nil {
			return nil, err
		}

		if err := saveToken(tokenFile, tok); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to save token: %v\n", err)
		}
	}

	// Wrap the TokenSource so refreshed tokens are persisted to disk. This
	// prevents losing the refresh token (or requiring interactive reauth)
	// when access tokens expire.
	baseSrc := config.TokenSource(ctx, tok)
	saveSrc := &fileSavingTokenSource{src: baseSrc, tokenFile: tokenFile, last: tok}

	return oauth2.NewClient(ctx, saveSrc), nil
}

// fileSavingTokenSource wraps an oauth2.TokenSource and saves refreshed tokens
// back to disk. It preserves an existing refresh token if the refresh reply
// omits it, preventing accidental loss of refresh tokens.
type fileSavingTokenSource struct {
	src       oauth2.TokenSource
	tokenFile string
	mu        sync.Mutex
	last      *oauth2.Token
}

func (f *fileSavingTokenSource) Token() (*oauth2.Token, error) {
	t, err := f.src.Token()
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Preserve refresh token if the returned token omitted it but we have it
	// from a previously saved token.
	if t.RefreshToken == "" && f.last != nil && f.last.RefreshToken != "" {
		t.RefreshToken = f.last.RefreshToken
	}

	needSave := false
	if f.last == nil {
		needSave = true
	} else if t.AccessToken != f.last.AccessToken || t.RefreshToken != f.last.RefreshToken || !t.Expiry.Equal(f.last.Expiry) {
		needSave = true
	}

	if needSave {
		_ = saveToken(f.tokenFile, t)
		f.last = t
	}

	return t, nil
}

func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

func saveToken(path string, token *oauth2.Token) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(token)
}

func ensureSecureCredsDir(credsDir string, logger *log.Logger) error {
	if err := os.MkdirAll(credsDir, 0700); err != nil {
		return fmt.Errorf("failed to create creds dir %s: %w", credsDir, err)
	}

	known := []string{
		"creds_src_oauth.json",
		"creds_src.json",
		"creds_dst_oauth.json",
		"creds_dst.json",
		"src_token.json",
		"dst_token.json",
		"creds_src_oauth.json",
		"creds_dst_oauth.json",
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	for _, name := range known {
		srcPath := filepath.Join(cwd, name)
		dstPath := filepath.Join(credsDir, name)

		if _, err := os.Stat(dstPath); err == nil {
			continue
		}

		if _, err := os.Stat(srcPath); err != nil {
			continue
		}

		if err := os.Rename(srcPath, dstPath); err != nil {
			in, rerr := os.Open(srcPath)
			if rerr != nil {
				logger.Printf("warning: failed to move credential %s: %v", srcPath, rerr)
				continue
			}

			out, werr := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY, 0600)
			if werr != nil {
				in.Close()
				logger.Printf("warning: failed to copy credential to %s: %v", dstPath, werr)
				continue
			}

			if _, err := io.Copy(out, in); err != nil {
				logger.Printf("warning: failed to copy credential content for %s: %v", name, err)
			}

			in.Close()
			out.Close()

			_ = os.Remove(srcPath)
		} else {
			// successfully renamed; set secure mode
			_ = os.Chmod(dstPath, 0600)
		}
		// ensure secure permissions on the file
		_ = os.Chmod(dstPath, 0600)
	}

	return nil
}

func probeDestination(ctx context.Context, svc *drive.Service, name string, contains bool, logger *log.Logger) {
	q := fmt.Sprintf("name = '%s' and trashed = false", escapeQuery(name))
	if contains {
		q = fmt.Sprintf("name contains '%s' and trashed = false", escapeQuery(name))
	}

	logger.Printf("probing destination with query: %s\n", q)

	var r *drive.FileList
	err := withRateLimit(ctx, "", logger, func() error {
		var e error
		r, e = svc.Files.List().Q(q).Fields("files(id,name,parents, mimeType)").Do()
		return e
	})

	if err != nil {
		logger.Fatalf("probe failed: %v", err)
	}

	if len(r.Files) == 0 {
		logger.Println("no matches found")
		return
	}

	for _, f := range r.Files {
		logger.Printf("found: %s (id=%s, mime=%s) parents=%v\n", f.Name, f.Id, f.MimeType, f.Parents)
	}
}

func escapeQuery(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

func resolvePathToFolderID(ctx context.Context, svc *drive.Service, path string) (string, error) {
	parts := strings.Split(path, "/")
	parent := "root"

	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		q := fmt.Sprintf("name = '%s' and mimeType = 'application/vnd.google-apps.folder' and '%s' in parents and trashed = false", escapeQuery(p), parent)

		var res *drive.FileList
		err := withRateLimit(ctx, "", nil, func() error {
			var e error
			res, e = svc.Files.List().Q(q).Fields("files(id,name)").Do()
			return e
		})

		if err != nil {
			return "", err
		}

		if len(res.Files) == 0 {
			return "", nil
		}

		parent = res.Files[0].Id

	}

	return parent, nil
}

// parseDriveFolderLink attempts to extract a folder ID from several Drive link formats.
func parseDriveFolderLink(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	// common formats:
	// https://drive.google.com/drive/folders/<id>
	// https://drive.google.com/open?id=<id>
	// https://drive.google.com/drive/u/0/folders/<id>
	// share links may contain id=<id>
	if strings.Contains(s, "drive.google.com") {
		if idx := strings.Index(s, "/folders/"); idx != -1 {
			part := s[idx+len("/folders/"):]

			for i, ch := range part {
				if ch == '/' || ch == '?' || ch == '&' || ch == '#' {
					return part[:i]
				}
			}

			return part
		}

		if idx := strings.Index(s, "id="); idx != -1 {
			part := s[idx+3:]
			for i, ch := range part {
				if ch == '&' || ch == '#' || ch == '/' || ch == '?' {
					return part[:i]
				}
			}

			return part
		}
	}

	return ""
}

// resolveFolderIDToPath walks parents from a folder ID up to 'root' to build a readable path under My Drive.
func resolveFolderIDToPath(ctx context.Context, svc *drive.Service, folderID string) (string, error) {
	if folderID == "" || folderID == "root" {
		return "", nil
	}

	var parts []string
	cur := folderID

	for cur != "" && cur != "root" {
		var meta *drive.File
		if err := withRateLimit(ctx, "", nil, func() error {
			var e error
			meta, e = svc.Files.Get(cur).Fields("id,name,parents").Do()
			return e
		}); err != nil {
			return "", err
		}

		if meta == nil {
			break
		}

		parts = append([]string{meta.Name}, parts...)
		if len(meta.Parents) == 0 {
			break
		}

		// take first parent; Drive supports multiple parents but My Drive typical case uses one
		cur = meta.Parents[0]
		if cur == "root" {
			break
		}
	}

	if len(parts) == 0 {
		return "", nil
	}

	return strings.Join(parts, "/"), nil
}

func migrateRootFiles(ctx context.Context, src, dst *drive.Service, dstEmail string, logger *log.Logger) {
	q := "'root' in parents and trashed = false and mimeType != 'application/vnd.google-apps.folder'"

	if verbose {
		logger.Printf("listing root files with query: %s", q)
	}

	var r *drive.FileList
	if err := withRateLimit(ctx, "", logger, func() error {
		var e error
		// include size and md5Checksum so we can persist into DB and do duplicate checks
		r, e = src.Files.List().Q(q).Fields("files(id,name,mimeType,parents,size,md5Checksum)").Do()
		return e
	}); err != nil {
		logger.Fatalf("failed to list root files: %v", err)
	}

	for _, f := range r.Files {
		stats.FilesTotal++
		if dryRun {
			stats.FilesPlanned++
		}

		// Skip Google-native files (Docs/Sheets/Slides/etc.) — these require export handling
		if strings.HasPrefix(f.MimeType, "application/vnd.google-apps.") {
			stats.FilesSkippedNative++
			if verbose {
				logger.Printf("skipping native Google file at root: %s (mime=%s)", f.Name, f.MimeType)
			}

			continue
		}

		// update current folder context for status emails
		currentFolderMu.Lock()
		currentFolder = "root"
		currentFolderMu.Unlock()
		logger.Printf("file: %s (id=%s) -> copying to destination root", f.Name, f.Id)

		if dryRun {
			continue
		}

		status, _, _, derr := dbGetStatus(f.Id)
		if derr != nil {
			logger.Printf("warning: failed to query DB for %s: %v", f.Id, derr)
		}

		if status == "done" {
			if verbose {
				logger.Printf("skipping %s because DB indicates done", f.Name)
			}
			continue
		}

		// mark in progress
		if err := dbMarkInProgress(f.Id, f.Name, f.Size); err != nil {
			logger.Printf("warning: failed to mark in-progress for %s: %v", f.Id, err)
		}

		b, dstID, err := copyFile(ctx, src, dst, f.Id, f.Name, []string{"root"}, dstEmail, logger)
		if err != nil {
			stats.FilesFailed++
			_ = dbMarkFailed(f.Id, err.Error())
			if verbose {
				logger.Printf("failed copying %s: %v", f.Name, err)
			}
			continue
		}

		if derr := dbMarkDone(f.Id, dstID, b); derr != nil {
			logger.Printf("warning: failed to mark done in DB for %s: %v", f.Id, derr)
		}

		stats.FilesCopied++
		stats.BytesCopied += b
		printETA(logger)
	}
}

func migrateFolderRecursive(ctx context.Context, src, dst *drive.Service, srcFolderID, dstParentID string, dstEmail string, logger *log.Logger, parentPath string) {
	// List child folders
	fQ := fmt.Sprintf("'%s' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false", srcFolderID)
	if verbose {
		logger.Printf("listing folders with query: %s", fQ)
	}

	var folders *drive.FileList
	if err := withRateLimit(ctx, "", logger, func() error {
		var e error
		folders, e = src.Files.List().Q(fQ).Fields("files(id,name)").Do()
		return e
	}); err != nil {
		logger.Fatalf("failed to list folders: %v", err)
	}

	// Process files in this folder
	fileQ := fmt.Sprintf("'%s' in parents and trashed = false and mimeType != 'application/vnd.google-apps.folder'", srcFolderID)
	if verbose {
		logger.Printf("listing files with query: %s", fileQ)
	}

	var files *drive.FileList
	if err := withRateLimit(ctx, "", logger, func() error {
		var e error
		files, e = src.Files.List().Q(fileQ).Fields("files(id,name,mimeType)").Do()
		return e
	}); err != nil {
		logger.Fatalf("failed to list files: %v", err)
	}

	// Ensure destination folder exists (dstParentID may be 'root' or another folder id)
	// If srcFolderID corresponds to a named folder, create matching folder in dst under dstParentID
	// Try to get name of src folder
	var srcMeta *drive.File
	if err := withRateLimit(ctx, "", logger, func() error {
		var e error
		srcMeta, e = src.Files.Get(srcFolderID).Fields("id,name").Do()
		return e
	}); err != nil {
		logger.Fatalf("failed to get src folder meta: %v", err)
	}

	name := srcMeta.Name
	// Build full path for status logging/emails
	var fullPath string
	if parentPath == "root" {
		fullPath = "root/" + name
	} else {
		fullPath = parentPath + "/" + name
	}
	currentFolderMu.Lock()
	currentFolder = fullPath
	currentFolderMu.Unlock()
	if slices.Contains(excludes, name) {
		if verbose {
			logger.Printf("skipping excluded folder %s", name)
		}
		return
	}

	// create or find dst folder under dstParentID
	dstFolderID := dstFindOrCreateFolder(ctx, dst, name, dstParentID, logger)

	for _, f := range files.Files {
		stats.FilesTotal++
		if dryRun {
			stats.FilesPlanned++
		}

		// Skip Google-native files (Docs/Sheets/Slides/etc.) — these require export handling
		if strings.HasPrefix(f.MimeType, "application/vnd.google-apps.") {
			stats.FilesSkippedNative++
			if verbose {
				logger.Printf("skipping native Google file: %s (mime=%s)", f.Name, f.MimeType)
			}
			continue
		}

		logger.Printf("copying file %s (id=%s) into dst folder %s", f.Name, f.Id, dstFolderID)
		if dryRun {
			continue
		}

		status, _, _, derr := dbGetStatus(f.Id)
		if derr != nil {
			logger.Printf("warning: failed to query DB for %s: %v", f.Id, derr)
		}

		if status == "done" {
			if verbose {
				logger.Printf("skipping %s because DB indicates done", f.Name)
			}
			continue
		}

		if err := dbMarkInProgress(f.Id, f.Name, f.Size); err != nil {
			logger.Printf("warning: failed to mark in-progress for %s: %v", f.Id, err)
		}

		b, dstID, err := copyFile(ctx, src, dst, f.Id, f.Name, []string{dstFolderID}, dstEmail, logger)
		if err != nil {
			stats.FilesFailed++
			_ = dbMarkFailed(f.Id, err.Error())
			if verbose {
				logger.Printf("failed copying %s: %v", f.Name, err)
			}
			continue
		}

		if derr := dbMarkDone(f.Id, dstID, b); derr != nil {
			logger.Printf("warning: failed to mark done in DB for %s: %v", f.Id, derr)
		}

		stats.FilesCopied++
		stats.BytesCopied += b
		printETA(logger)
	}

	// Recurse into subfolders
	for _, fo := range folders.Files {
		migrateFolderRecursive(ctx, src, dst, fo.Id, dstFolderID, dstEmail, logger, fullPath)
	}
}

func dstFindOrCreateFolder(ctx context.Context, svc *drive.Service, name, parentID string, logger *log.Logger) string {
	q := fmt.Sprintf("name = '%s' and mimeType = 'application/vnd.google-apps.folder' and '%s' in parents and trashed = false", escapeQuery(name), parentID)
	if verbose {
		logger.Printf("dst folder query: %s", q)
	}

	var r *drive.FileList
	if err := withRateLimit(ctx, "", logger, func() error {
		var e error
		r, e = svc.Files.List().Q(q).Fields("files(id,name)").Do()
		return e
	}); err == nil && len(r.Files) > 0 {
		return r.Files[0].Id
	}

	fld := &drive.File{
		Name:     name,
		MimeType: "application/vnd.google-apps.folder",
		Parents:  []string{parentID},
	}

	if verbose {
		logger.Printf("creating dst folder %s under parent %s", name, parentID)
	}

	var created *drive.File
	if err := limitedCall(ctx, "", logger, func() error {
		var e error
		created, e = svc.Files.Create(fld).Fields("id").Do()
		return e
	}); err != nil {
		logger.Fatalf("failed to create dst folder %s: %v", name, err)
	}

	return created.Id
}

// dstFindDuplicate searches the destination folder(s) for a file that likely matches the source file.
// Matching strategy: prefer md5Checksum match when available, otherwise match by name+size.
func dstFindDuplicate(ctx context.Context, svc *drive.Service, name string, parents []string, size int64, md5 string) (string, bool) {
	// Search parents one by one for simplicity
	for _, p := range parents {
		var q string
		if md5 != "" {
			q = fmt.Sprintf("name = '%s' and md5Checksum = '%s' and '%s' in parents and trashed = false", escapeQuery(name), md5, p)
		} else if size > 0 {
			q = fmt.Sprintf("name = '%s' and size = %d and '%s' in parents and trashed = false", escapeQuery(name), size, p)
		} else {
			q = fmt.Sprintf("name = '%s' and '%s' in parents and trashed = false", escapeQuery(name), p)
		}

		var r *drive.FileList
		if err := withRateLimit(ctx, "", nil, func() error {
			var e error
			r, e = svc.Files.List().Q(q).Fields("files(id,name,size,md5Checksum)").Do()
			return e
		}); err != nil {
			continue
		}

		if len(r.Files) > 0 {
			return r.Files[0].Id, true
		}
	}

	return "", false
}

// printETA logs an estimation of remaining time based on bytes copied so far and elapsed time.
func printETA(logger *log.Logger) {
	_ = logger
}

// startStatusLogger periodically logs elapsed time and basic counters so runs remain observable
func startStatusLogger(ctx context.Context, logger *log.Logger, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(stats.Start)
				logger.Printf("status: elapsed=%s files_total=%d files_copied=%d files_failed=%d bytes_copied=%s",
					elapsed.Truncate(time.Second), stats.FilesTotal, stats.FilesCopied, stats.FilesFailed, formatBytes(stats.BytesCopied))
			}
		}
	}()
}

// sendProgressEmail composes and sends a plain-text status email from the destination account to the source account.
func sendProgressEmail(ctx context.Context, gmailSvc *gmail.Service, fromEmail, toEmail string, logger *log.Logger) error {
	if gmailSvc == nil {
		return fmt.Errorf("gmail service is nil")
	}

	// snapshot stats
	currentFolderMu.Lock()
	cur := currentFolder
	currentFolderMu.Unlock()

	elapsed := time.Since(stats.Start)
	remaining := "unknown"
	if stats.FilesTotal > 0 {
		rem := max(stats.FilesTotal-stats.FilesCopied, 0)
		remaining = fmt.Sprintf("%d", rem)
	}

	body := fmt.Sprintf("GDrive migration status report\n\nTime: %s\nElapsed: %s\nFiles completed: %d\nFiles remaining (observed): %s\nFiles failed: %d\nFiles skipped (native): %d\nBytes copied: %s\nCurrent source folder: %s\n\nNote: 'Files remaining' is based on files encountered so far and may be incomplete if the tool is still enumerating folders.\n",
		time.Now().Format(time.RFC1123), elapsed.Truncate(time.Second), stats.FilesCopied, remaining, stats.FilesFailed, stats.FilesSkippedNative, formatBytes(stats.BytesCopied), cur)

	var msg gmail.Message

	// If we have the first-message Message-ID, include In-Reply-To/References headers for better threading
	emailMessageMu.Lock()
	mid := emailMessageID
	emailMessageMu.Unlock()

	headers := ""
	if mid != "" {
		mval := mid
		if !strings.HasPrefix(mval, "<") {
			mval = fmt.Sprintf("<%s>", mval)
		}
		headers = fmt.Sprintf("In-Reply-To: %s\r\nReferences: %s\r\n", mval, mval)
	}

	raw := []byte(fmt.Sprintf("From: %s\r\nTo: %s\r\n%sSubject: GDrive migration status\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s", fromEmail, toEmail, headers, body))
	msg.Raw = base64.RawURLEncoding.EncodeToString(raw)

	emailThreadMu.Lock()
	if emailThreadID != "" {
		msg.ThreadId = emailThreadID
	}
	emailThreadMu.Unlock()

	var resp *gmail.Message
	err := withRateLimit(ctx, "", logger, func() error {
		var e error
		resp, e = gmailSvc.Users.Messages.Send("me", &msg).Do()
		return e
	})

	if err != nil {
		logger.Printf("failed to send status email: %v", err)
		return err
	}

	if resp != nil {
		emailThreadMu.Lock()
		if emailThreadID == "" && resp.ThreadId != "" {
			emailThreadID = resp.ThreadId

			if emailMessageID == "" {
				if m, ferr := gmailSvc.Users.Messages.Get("me", resp.Id).Format("full").Do(); ferr == nil && m != nil {
					for _, h := range m.Payload.Headers {
						if strings.EqualFold(h.Name, "Message-ID") || strings.EqualFold(h.Name, "Message-Id") {
							emailMessageMu.Lock()
							emailMessageID = h.Value
							emailMessageMu.Unlock()
							break
						}
					}
				}
			}
		}

		emailThreadMu.Unlock()
	}

	logger.Printf("sent status email from %s to %s (thread=%s)", fromEmail, toEmail, emailThreadID)
	return nil
}

// sendFinalEmail composes a verbose final report and sends it from destination account to source account.
func sendFinalEmail(ctx context.Context, gmailSvc *gmail.Service, fromEmail, toEmail string, logger *log.Logger) error {
	if gmailSvc == nil {
		return fmt.Errorf("gmail service is nil")
	}

	currentFolderMu.Lock()
	cur := currentFolder
	currentFolderMu.Unlock()

	elapsed := time.Since(stats.Start)

	var remaining int64 = -1
	if gormDB != nil {
		var cnt int64
		if err := gormDB.Model(&FileRecord{}).Where("status != ?", "done").Count(&cnt).Error; err == nil {
			remaining = cnt
		}
	}

	failures := []FileRecord{}
	if gormDB != nil {
		_ = gormDB.Where("status = ?", "failed").Limit(50).Find(&failures).Error
	}

	var b strings.Builder
	b.WriteString("GDrive migration - FINAL REPORT\n\n")
	b.WriteString(fmt.Sprintf("Time: %s\n", time.Now().Format(time.RFC1123)))
	b.WriteString(fmt.Sprintf("Elapsed: %s\n", elapsed.Truncate(time.Second)))
	b.WriteString(fmt.Sprintf("Files encountered: %d\n", stats.FilesTotal))

	if dryRun {
		b.WriteString(fmt.Sprintf("Files planned (dry-run): %d\n", stats.FilesPlanned))
	}

	b.WriteString(fmt.Sprintf("Files copied: %d\n", stats.FilesCopied))
	if remaining >= 0 {
		b.WriteString(fmt.Sprintf("Files remaining (DB): %d\n", remaining))
	} else {
		b.WriteString("Files remaining (DB): unknown\n")
	}

	b.WriteString(fmt.Sprintf("Files failed: %d\n", stats.FilesFailed))
	b.WriteString(fmt.Sprintf("Files skipped (native): %d\n", stats.FilesSkippedNative))
	b.WriteString(fmt.Sprintf("Bytes copied: %s\n", formatBytes(stats.BytesCopied)))
	b.WriteString(fmt.Sprintf("Current source folder: %s\n\n", cur))

	if len(failures) > 0 {
		b.WriteString("Recent failures (up to 50):\n")

		for _, f := range failures {
			b.WriteString(fmt.Sprintf("- %s (src_id=%s) error: %s\n", f.SrcName, f.SrcID, f.LastError))
		}

		b.WriteString("\n")
	}

	body := b.String()

	var msg gmail.Message

	emailMessageMu.Lock()
	mid := emailMessageID
	emailMessageMu.Unlock()

	headers := ""
	if mid != "" {
		mval := mid
		if !strings.HasPrefix(mval, "<") {
			mval = fmt.Sprintf("<%s>", mval)
		}
		headers = fmt.Sprintf("In-Reply-To: %s\r\nReferences: %s\r\n", mval, mval)
	}

	raw := []byte(fmt.Sprintf("From: %s\r\nTo: %s\r\n%sSubject: GDrive migration - FINAL REPORT\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s", fromEmail, toEmail, headers, body))
	msg.Raw = base64.RawURLEncoding.EncodeToString(raw)

	emailThreadMu.Lock()
	if emailThreadID != "" {
		msg.ThreadId = emailThreadID
	}

	emailThreadMu.Unlock()

	var resp *gmail.Message
	err := withRateLimit(ctx, "", logger, func() error {
		var e error
		resp, e = gmailSvc.Users.Messages.Send("me", &msg).Do()
		return e
	})

	if err != nil {
		logger.Printf("failed to send final status email: %v", err)
		return err
	}

	// Save thread ID and message-id if not already set
	if resp != nil {
		emailThreadMu.Lock()
		if emailThreadID == "" && resp.ThreadId != "" {
			emailThreadID = resp.ThreadId
		}
		emailThreadMu.Unlock()

		if emailMessageID == "" {
			if m, ferr := gmailSvc.Users.Messages.Get("me", resp.Id).Format("full").Do(); ferr == nil && m != nil {
				for _, h := range m.Payload.Headers {
					if strings.EqualFold(h.Name, "Message-ID") || strings.EqualFold(h.Name, "Message-Id") {
						emailMessageMu.Lock()
						emailMessageID = h.Value
						emailMessageMu.Unlock()
						break
					}
				}
			}
		}
	}

	logger.Printf("sent final status email from %s to %s (thread=%s)", fromEmail, toEmail, emailThreadID)
	return nil
}

// getUserLimiter returns or creates a per-user limiter.
func getUserLimiter(email string) *rate.Limiter {
	if email == "" {
		return nil
	}

	userLimitersMu.Lock()
	defer userLimitersMu.Unlock()
	if l, ok := userLimiters[email]; ok {
		return l
	}

	l := rate.NewLimiter(rate.Limit(defaultUserQPS), defaultUserBurst)
	userLimiters[email] = l
	return l
}

// isRetryableError inspects an error and decides whether to retry.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	if ge, ok := err.(*googleapi.Error); ok {
		if ge.Code == 429 {
			return true
		}
		if ge.Code >= 500 && ge.Code <= 599 {
			return true
		}
		if ge.Code == 403 {
			for _, e := range ge.Errors {
				if e.Reason == "userRateLimitExceeded" || e.Reason == "rateLimitExceeded" || e.Reason == "backendError" {
					return true
				}
			}
		}
	}

	// fallback: if the error has Temporary() bool, trust it
	if te, ok := err.(interface{ Temporary() bool }); ok {
		return te.Temporary()
	}

	return false
}

// withRateLimit runs the provided call under project and per-user rate limiters and retries on transient errors.
func withRateLimit(ctx context.Context, user string, logger *log.Logger, call func() error) error {
	// If noRateLimit is set, skip acquiring tokens from our internal limiters
	if !noRateLimit {
		if projectLimiter != nil {
			if err := projectLimiter.Wait(ctx); err != nil {
				return err
			}
		}
		if ul := getUserLimiter(user); ul != nil {
			if err := ul.Wait(ctx); err != nil {
				return err
			}
		}
	}

	var lastErr error
	base := time.Second
	maxBackoff := 32 * time.Second
	maxAttempts := 8

	for attempt := range maxAttempts {
		err := call()
		if err == nil {
			return nil
		}

		lastErr = err
		if !isRetryableError(err) {
			return err
		}

		// exponential backoff with full jitter
		maxWait := min(base*(1<<attempt), maxBackoff)
		sleep := max(time.Duration(rand.Int63n(int64(maxWait))), 200*time.Millisecond)
		if logger != nil {
			logger.Printf("retryable error (attempt %d/%d): %v; sleeping %s", attempt+1, maxAttempts, err, sleep)
		}

		select {
		case <-time.After(sleep):
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return lastErr
}

// limitedCall acquires limiter tokens and executes the call exactly once (no retry).
func limitedCall(ctx context.Context, user string, logger *log.Logger, call func() error) error {
	if noRateLimit {
		return call()
	}

	if projectLimiter != nil {
		if err := projectLimiter.Wait(ctx); err != nil {
			return err
		}
	}

	if ul := getUserLimiter(user); ul != nil {
		if err := ul.Wait(ctx); err != nil {
			return err
		}
	}

	return call()
}

func copyFile(ctx context.Context, srcSvc, dstSvc *drive.Service, srcFileID, name string, dstParents []string, dstEmail string, logger *log.Logger) (int64, string, error) {
	// Use Files.Get to fetch metadata and downloadable link
	srcF, err := srcSvc.Files.Get(srcFileID).Fields("id,name,mimeType,parents,trashed,webContentLink,size,md5Checksum").Do()
	if err != nil {
		logger.Printf("failed to get source file %s: %v", srcFileID, err)
		return 0, "", err
	}

	if srcF.MimeType == "application/vnd.google-apps.folder" {
		logger.Printf("skipping folder in copyFile: %s", srcF.Name)
		return 0, "", fmt.Errorf("source is a folder")
	}

	if strings.HasPrefix(srcF.MimeType, "application/vnd.google-apps.") {
		logger.Printf("skipping native Google file (requires export): %s (mime=%s)", srcF.Name, srcF.MimeType)
		return 0, "", fmt.Errorf("native google file, skipped")
	}

	if dstID, ok := dstFindDuplicate(ctx, dstSvc, srcF.Name, dstParents, srcF.Size, srcF.Md5Checksum); ok {
		logger.Printf("duplicate detected in destination for %s -> skipping copy (dst id=%s)", srcF.Name, dstID)
		return srcF.Size, dstID, nil
	}

	if verbose {
		logger.Printf("attempting server-side copy for %s", srcFileID)
	}

	errGet := withRateLimit(ctx, "", logger, func() error {
		_, e := dstSvc.Files.Get(srcFileID).Fields("id").Do()
		return e
	})

	if errGet == nil {
		// destination can access source file directly
		var created *drive.File
		cerr := withRateLimit(ctx, "", logger, func() error {
			var e error
			created, e = dstSvc.Files.Copy(srcFileID, &drive.File{Name: name, Parents: dstParents}).Fields("id").Do()
			return e
		})

		if cerr == nil {
			logger.Printf("server-side copied %s (id=%s)", name, created.Id)
			if srcF.Size > 0 {
				return srcF.Size, created.Id, nil
			}
			return 0, created.Id, nil
		}

		logger.Printf("server-side copy failed for %s: %v", name, cerr)
	} else {
		// If permission denied, try granting temporary reader permission on the source file to dstEmail
		if gerr, ok := errGet.(*googleapi.Error); ok && (gerr.Code == 403 || gerr.Code == 404) {
			if dstEmail != "" {
				if verbose {
					logger.Printf("attempting to grant temporary permission on source %s to %s", srcFileID, dstEmail)
				}

				perm := &drive.Permission{Type: "user", Role: "reader", EmailAddress: dstEmail}
				var pc *drive.Permission
				perr := withRateLimit(ctx, "", logger, func() error {
					var e error
					pc, e = srcSvc.Permissions.Create(srcFileID, perm).SendNotificationEmail(false).Do()
					return e
				})

				if perr == nil {
					// try server-side copy now
					var created *drive.File
					cerr := withRateLimit(ctx, "", logger, func() error {
						var e error
						created, e = dstSvc.Files.Copy(srcFileID, &drive.File{Name: name, Parents: dstParents}).Fields("id").Do()
						return e
					})

					if cerr == nil {
						logger.Printf("server-side copied %s (id=%s) after granting permission", name, created.Id)
						// revoke permission
						_ = withRateLimit(ctx, "", logger, func() error {
							return srcSvc.Permissions.Delete(srcFileID, pc.Id).Do()
						})
						if srcF.Size > 0 {
							return srcF.Size, created.Id, nil
						}
						return 0, created.Id, nil
					} else {
						logger.Printf("server copy failed after granting permission for %s: %v", name, cerr)
						// revoke permission even if copy failed
						_ = withRateLimit(ctx, "", logger, func() error {
							return srcSvc.Permissions.Delete(srcFileID, pc.Id).Do()
						})
					}
				} else {
					logger.Printf("failed to create temporary permission on %s for %s: %v", srcFileID, dstEmail, perr)
				}
			}
		}
	}

	// Fallback to streaming download/upload through this machine
	var resp *http.Response
	if err := limitedCall(ctx, "", logger, func() error {
		var e error
		resp, e = srcSvc.Files.Get(srcFileID).Download()
		return e
	}); err != nil {
		logger.Printf("failed to download file %s: %v", srcF.Name, err)
		return 0, "", err
	}

	defer resp.Body.Close()

	dstMetadata := &drive.File{Name: name, Parents: dstParents}

	// Upload: for large files use resumable uploads, otherwise use simple upload
	const resumableThreshold int64 = 5 << 20 // 5 MB
	contentType := srcF.MimeType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	if srcF.Size >= resumableThreshold {
		if verbose {
			logger.Printf("starting resumable upload for %s (size=%d)", name, srcF.Size)
		}

		tmp, terr := os.CreateTemp("", "gdrive-migrate-*")
		if terr != nil {
			logger.Printf("failed to create temp file for resumable upload: %v", terr)
			return 0, "", terr
		}

		if _, err := io.Copy(tmp, resp.Body); err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			logger.Printf("failed to write temp file for %s: %v", name, err)
			return 0, "", err
		}

		// reopen for read
		if _, err := tmp.Seek(0, io.SeekStart); err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			return 0, "", err
		}

		// use the file for resumable upload
		call := dstSvc.Files.Create(dstMetadata)
		created, err := call.ResumableMedia(ctx, tmp, srcF.Size, contentType).Fields("id").Do()
		tmp.Close()
		os.Remove(tmp.Name())
		if err != nil {
			logger.Printf("resumable upload failed for %s: %v", name, err)
			return 0, "", err
		}

		logger.Printf("created dst file %s (id=%s) via resumable upload", name, created.Id)
		return srcF.Size, created.Id, nil
	} else {
		if verbose {
			logger.Printf("uploading %s to destination (parents=%v)", name, dstParents)
		}

		created, err := dstSvc.Files.Create(dstMetadata).Media(resp.Body).Fields("id").Do()
		if err != nil {
			logger.Printf("failed to create file in destination for %s: %v", name, err)
			return 0, "", err
		}

		logger.Printf("created dst file %s (id=%s)", name, created.Id)
		return srcF.Size, created.Id, nil
	}
}
