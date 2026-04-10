# Git Workflow for sb-storage-sdk

This repository contains the Slidebolt Storage SDK, providing the interfaces and client implementation for data persistence within the Slidebolt ecosystem.

## Dependencies
- **Internal:**
  - `sb-messenger-sdk`: Used for communication with the storage server.
- **External:** 
  - `github.com/nats-io/nats.go`: Communication with NATS.

## Build Process
- **Type:** Pure Go Library (Shared Module).
- **Consumption:** Imported as a module dependency in other Go projects via `go.mod`.
- **Artifacts:** No standalone binary or executable is produced.
- **Validation:** 
  - Validated through unit tests: `go test -v ./...`
  - Validated by its consumers during their respective build/test cycles.

## Pre-requisites & Publishing
As a storage client library, `sb-storage-sdk` should be updated whenever the messaging SDK (`sb-messenger-sdk`) is updated.

**Before publishing:**
1. Determine current tag: `git tag | sort -V | tail -n 1`
2. Ensure all local tests pass: `go test -v ./...`

**Publishing Order:**
1. Ensure `sb-messenger-sdk` is tagged and pushed (e.g., `v1.0.4`).
2. Update `sb-storage-sdk/go.mod` to reference the latest `sb-messenger-sdk` tag.
3. Determine next semantic version for `sb-storage-sdk` (e.g., `v1.0.4`).
4. Commit and push the changes to `main`.
5. Tag the repository: `git tag v1.0.4`.
6. Push the tag: `git push origin main v1.0.4`.
7. Update dependent repositories using `go get github.com/slidebolt/sb-storage-sdk@v1.0.4`.

## Update Workflow & Verification
1. **Modify:** Update storage client logic in `storage.go`, `query.go`, or `watch.go`.
2. **Verify Local:**
   - Run `go mod tidy`.
   - Run `go test ./...`.
3. **Commit:** Ensure the commit message clearly describes the storage change.
4. **Tag & Push:** (Follow the Publishing Order above).
