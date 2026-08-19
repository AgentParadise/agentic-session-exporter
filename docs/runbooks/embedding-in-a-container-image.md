# Runbook: embedding the exporter in a container image

For image authors who want capture available inside a workspace or job
container.

## Copy from the OCI image, by digest

```dockerfile
COPY --from=ghcr.io/agentparadise/agentic-session-exporter@sha256:<digest> \
     /apss-session-exporter /usr/local/bin/apss-session-exporter
```

Three deliberate choices:

**An OCI image, not a release asset.** `COPY --from=` cannot consume a GitHub
release. The alternative - `curl` at build time - puts an unverified binary in
your image and makes the build depend on a download.

**By digest, never by tag.** A tag is mutable. Pinning one means an upstream push
silently changes what your image ships, which is exactly how a regression
reaches production images that nobody rebuilt.

**A `FROM scratch` source image.** It exists to be copied *from*, never run, so
it contributes no attack surface.

## Verify the image before bumping the pin

```bash
cosign verify \
  --certificate-identity-regexp 'https://github.com/AgentParadise/agentic-session-exporter/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/agentparadise/agentic-session-exporter@sha256:<digest>
```

Do this when you change the digest, not once when you first adopt it. The point
of pinning is that each new digest is a decision.

## Multi-arch

The image carries `linux/amd64` and `linux/arm64`. `COPY --from=` selects the
matching architecture automatically when your build sets `TARGETARCH`, so a
multi-arch consumer needs no per-arch branching.

## Invoking it

Give it the environment and call it. It needs no daemon and no init:

```bash
SESSION_STORE_URL=… SESSIONS_WRITE_TOKEN=… apss-session-exporter
```

Two things a supervisor should know:

**Probe with `--version`, and know what your version does.** It is meant to be
side-effect free. Verify that for the version you ship rather than assuming it:
a probe that performs a sweep turns a health check into an upload, and that has
happened.

**Give it a real time budget.** If you invoke it on a container-stop path, the
stop deadline is the whole budget. A sweep that needs longer than you allow is
killed mid-upload, and whatever was not accepted is lost with the container.

## Where transcripts live matters more than it looks

If your spool is on a tmpfs or any filesystem that dies with the container, an
un-uploaded transcript dies with it. That is a durability decision, not a
detail: put the spool somewhere that outlives the container if you want a failed
upload to be recoverable.
