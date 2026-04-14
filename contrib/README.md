# contrib/

Deployment helpers that aren't part of the build.

## jmestrap.service — systemd unit

A unit file for running JMESTrap as a long-lived service on a lab rig,
with auto-restart on failure and logs routed to the journal.

### Install

```bash
# 1. Build and install the binary
cargo build --release --features mqtt    # or without --features for REST-only
sudo install -m 755 target/release/jmestrap /usr/local/bin/jmestrap

# 2. Create an unprivileged user for the service
sudo useradd --system --no-create-home --shell /usr/sbin/nologin jmestrap

# 3. Drop in the unit and enable it
sudo install -m 644 contrib/jmestrap.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now jmestrap
```

### Operating

```bash
systemctl status jmestrap            # is it up?
journalctl -u jmestrap -f            # tail logs live
journalctl -u jmestrap --since "1 hour ago"
sudo systemctl restart jmestrap      # after replacing the binary
```

### Tuning

Edit `/etc/systemd/system/jmestrap.service`, then
`sudo systemctl daemon-reload && sudo systemctl restart jmestrap`.

- **`--bind 0.0.0.0`** — required if DUTs on the rig LAN need to reach the
  service. Use `127.0.0.1` if only the host talks to it.
- **`--ttl 3600`** — per-recording TTL in seconds. Bump if your test
  scenarios can exceed 1 hour; this is the main bound on memory use under
  continuous operation.
- **MQTT ingress** — uncomment the `--mqtt ...` line in the unit and set
  broker address / subscription topics to match your rig.

### Hardening notes

The unit enables `ProtectSystem=strict`, which makes `/usr`, `/etc`, `/boot`
read-only for the service. This is fine today because JMESTrap writes
nothing to disk. If a persistence feature is added later, switch to
`ReadWritePaths=/var/lib/jmestrap` (or similar).
