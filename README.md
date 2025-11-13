# gdrive-migrate

A crude, sort of terribly written CLI tool to migrate files between Google Drive accounts. Features: server-side copy when possible, resumable upload fallback, checkpointing (SQLite), and optional periodic status emails.

Quick start

- Build:

```powershell
cd C:\DATA\GRAPEVINE\Programming\Go\gdrive-migrate
go build -v -o gdrive-migrate.exe
```

- Create a `state/creds` directory and place your OAuth client JSONs and tokens there. Example files the tool looks for:
  - `state/creds/creds_src_oauth.json` and `state/creds/src_token.json`
  - `state/creds/creds_dst_oauth.json` and `state/creds/dst_token.json`

- Dry-run (no writes):

```powershell
.\gdrive-migrate.exe -dry-run -v -start-path "Pictures/2024"
```

Notes

- If you enable `-status-email`, the destination account needs Gmail send scope. If an existing token lacks that scope you must re-authenticate interactively (remove `state/creds/dst_token.json` or run the tool interactively to perform OAuth consent).
- Credentials and tokens are sensitive; `.gitignore` excludes `state/creds` and token files by default.
- For Docker usage, mount the repository `state` into the container so tokens and the checkpoint DB persist.

For more options and flags run `.\gdrive-migrate.exe -h`.
