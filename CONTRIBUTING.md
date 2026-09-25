# Contributing

Contributions are welcome when they keep Resumexfer's recovery guarantees intact.

## Before opening a pull request

1. Keep transfers fail-closed when ownership or identity is uncertain.
2. Do not weaken prefix/source verification to gain speed.
3. Add a regression test for the user-visible failure being fixed.
4. Check the mirror direction and equivalent transport path, not only the reported case.
5. Run:

```bash
go test ./...
go vet ./...
python3 -m unittest discover -s nemo -p 'test_*.py'
python3 scripts/public-safety-check.py
```

For transfer-engine changes, also run the relevant scripts under `scripts/test-batches/`.

## Platform contributions

- Linux USB/MTP changes need GVfs/Nemo regression coverage.
- Windows/macOS local-network changes must keep `cmd/resumexfer` cross-compiling.
- Native Explorer/Finder MTP work should be isolated behind platform-specific packages rather than adding OS checks throughout the portable portal engine.

## Security

Do not include credentials, private capability links, personal filesystem paths, or private logs in commits/issues. Security vulnerabilities should use GitHub's private vulnerability reporting flow.
