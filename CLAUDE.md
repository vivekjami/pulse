# Pulse — working rules for the `api` service

`api` is the source-of-truth service: the whole cargo workspace and the `web/`
SPA live here, and `zerops.yaml` gives it `deployFiles: [.]`. The other three
services (`ingest`, `detector`, `web`) cross-deploy binaries and `web/dist` out
of this same tree.

## A deploy of `api` destroys everything uncommitted in this tree

The mount at `/var/www/api/` **is** the api container's filesystem, and a deploy
replaces that filesystem wholesale with the build container's output. The build
container is built from the source snapshot taken when the deploy started, so
anything written to the mount after that point — new files, edits, both — is
gone the moment the deploy lands. `git status` comes back clean and it looks
like the work was never done.

This already happened once: a deploy started at 11:01 landed at ~11:38 and took
out five new modules and ten modified files written in between.

So:

- **Commit before starting any `api` deploy**, and do not write to the tree
  while one is in flight.
- Recovery, if it happens again: `git diff` output captured in a tool-results
  file restores modified tracked files via `git apply`; new files have to be
  replayed out of the session transcript at
  `~/.claude/projects/-var-www/<session>.jsonl`.

And do not trust `.git` on this mount either. Commits made here have been lost
whole: three commits reported their hashes, then their objects vanished and the
reflog skipped them, because the mount is SSHFS and a container cycle discarded
buffered writes. Committing is not a durable act on this filesystem.

## So: work in a local copy, treat the mount as a deploy staging area

The reliable shape, and the one this repo's history was rebuilt with:

1. Keep the real working tree + `.git` on local disk (a scratchpad outside the
   mount). Edit and commit there.
2. `rsync -a --delete --exclude=target --exclude=node_modules --exclude=web/dist
   --exclude=raw <local>/ /var/www/api/` immediately before deploying.
3. Deploy. Build and test over `ssh api` against the synced tree.

Two traps in that loop, both already hit:

- The rsync carries `.git` too, so anything created **only** on the mount —
  notably `git tag` — is destroyed by the next sync. Create tags in the local
  copy, not here.
- `node_modules` is excluded, so after any wipe or sync the mount cannot run a
  web build. That is fine: Zerops' build container runs its own `npm ci`, and
  local builds happen in the local copy.

## Building and testing

No cargo on the ZCP host — run it in the service: `ssh api "cd /var/www && cargo
test --workspace -j 2"`. The `-j 2` matters: `/proc/loadavg` and the core count
leak the host's values into the container, so cargo otherwise sizes its job pool
to ~32 jobs against 3 CPUs and the container thrashes until SSH pipes drop.
Write long output to a file in the container and read it back, so a dropped pipe
doesn't lose the result.

`node`/`npm` are on the ZCP host but not in the `api` or `web` containers, so the
SPA typecheck and build run here: `cd /var/www/api/web && npm run build`.

## Gate tunables are deliberately not in `zerops.yaml`

A key declared in `run.envVariables` is owned by that file and cannot be
overridden at service scope, which would turn every detector tuning change into
a ~9-minute Rust rebuild. The §4 defaults live in the code; override them with
`zerops_env set serviceHostname=detector`, which restarts in seconds.

## Untrusted input

Every title, username and edit comment comes off a public wiki and is
attacker-controlled. In the SPA that means `textContent` only — never build
markup from stream data — and `rel="noopener noreferrer"` on outbound links.
