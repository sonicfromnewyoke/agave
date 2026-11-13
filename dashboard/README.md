# agave-dashboard

A web dashboard for an `agave-validator`, served by a process of its own beside it. Nothing of
it runs inside the validator: it follows the validator's ledger as a RocksDB secondary, and
reads its metrics export, its JSON-RPC and admin socket, and `/proc` of its process.

```bash
SOLANA_METRICS_CONFIG=host=http://127.0.0.1:18186,db=dashboard,u=dashboard,p=dashboard \
  agave-validator --rpc-port 8899 ...

agave-dashboard --ledger /path/to/ledger \
  --metrics-listen 127.0.0.1:18186 --vote-account <VOTE_PUBKEY> --listen 127.0.0.1:11000
```

Then open <http://127.0.0.1:11000>. The dashboard runs as the validator's user, on the same
host, since the admin socket is mode 0600 and the ledger, `/proc` and `/sys` are read in place.
It can be started and stopped at any time, and attaches to a validator already running; started
before the validator, it has the metrics export from the first point. `--metrics-forward` passes
the points on to an InfluxDB the validator reported to before. `--tip-payment-program` measures
Jito tips per block. `--rpc-url` names another RPC node of the cluster for the chain's state,
which spares the validator's own.

## Exposing it

The dashboard has no authentication and no TLS. It binds `127.0.0.1:11000` unless `--listen`
says otherwise, and everything below assumes you have decided to serve it to somebody else.

```bash
agave-dashboard ... --listen 0.0.0.0:11000 --allowed-host dashboard.example.com
```

`--allowed-host` lists the names the dashboard will answer to; repeat the flag
for each one. `localhost` and any IP literal are accepted without it, so
`http://<your-ip>:11000` works out of the box. A **domain** has to be listed.

A request whose `Host` is not on the list gets a **421** and the page does not
load at all. That is the failure to expect when a proxy forwards a name the
dashboard has not been told about, and the refused name is in its log.

Behind Caddy:

```caddy
dashboard.example.com {
	reverse_proxy 127.0.0.1:11000
}
```

Caddy passes the original `Host` through and upgrades websockets on its own, so
the domain in the site block is the name to allow, and nothing else is needed.

### What the server does on its own

- **Checks `Host`** against the list above before serving anything, so a name
  that resolves to your validator but that you did not list cannot reach the
  page. This is what stops DNS rebinding, which loopback binding does not.
- **Checks `Origin`** on the websocket. Websockets are exempt from the
  same-origin policy, so without this any page open in a viewer's browser could
  connect to a dashboard that viewer can reach.
- **Caps connections** at 256 being served at once, and websockets at 64 of
  those. A connection over the cap is closed without being read; a websocket
  over its own cap gets a 503.
- **Sends a content security policy** that permits no external code, styles,
  fonts or connections. Images are the one exception, because validator icons
  come from URLs operators publish on chain, and those are limited to `https`.
- **Bounds the work a caller can cause**: 8 KB request heads, 4 KB client
  messages, a 10s header timeout and a 15s write timeout.

### What it does not do

- No authentication, no TLS, no rate limiting, no access log. Those belong in
  the proxy.
- Validator icons are fetched by the viewer's browser from third-party hosts, so
  a viewer's IP address reaches whoever publishes those icons. That is inherent
  to displaying them.
- The identity and vote pubkeys, stake, version and skip rate of your validator
  are all on screen. All of it is already public on chain; none of it is secret,
  but it does identify which validator this is.
- The schedule page shows the stake, client version and gossip address of the
  other validators leading the slots on screen. All of that is already public,
  since every node in the cluster holds it, but serving the page publishes it to
  anyone who can reach it.
- The Gossip page lists every node in this validator's gossip table, with its
  stake, client, gossip address and RPC port. Every node holds the
  same table, so none of it is private, but the page gathers it in one place.
- The Cluster page totals the cluster's stake by client and by country, and
  names the leaders that skipped most this epoch. All of it is public on chain
  or in gossip.
- The Accounts page shows the accounts database's caches, storage files, clean
  and shrink and index, with the accounts paths. None of it is on chain.
- The machine card shows what is not on chain: the host's memory and the
  validator's share of it, its core count, load, CPU use, governor and swap,
  each disk's device name and fill, the absolute paths of the ledger, accounts
  and snapshot directories (cut short on the card, in full on hover) and the
  names of leftover directories beside them, the XDP interface's driver and
  model and the kernel version where XDP is on, the validator's thread names
  and the cores each is pinned to, and when a snapshot is being written.
- It lists by name every network interface but loopback that is up, routed or
  carrying traffic, with its kind, state, MTU, route count, traffic, the bond
  or bridge it belongs to and, for a tunnel, the interface carrying it. A VPN
  or private link is therefore visible by name.

### What it costs the validator

No code of the dashboard runs in the validator, and the validator is started
with no flag for it but the metrics export. Everything runs in the dashboard's
own process: the metrics listener, which answers each export at once and parses
it after, the ledger followed as a RocksDB secondary, RPC reads, and the `/proc`
walks behind the host, thread and socket panels, which run only while a viewer
is connected.

What it asks of the validator's RPC is the chain's state the ledger does not
hold, at about 1.3 calls a second: our identity and vote account in one read
every second, every vote account every five seconds, the gossip table every
ten. What changes only as an epoch begins is read once an epoch: features, rent,
the fee, the inflation account, the epoch's start stamp, the leader schedule
and the rank maps. Slots, transaction counts and the epoch's progress come from
the ledger and genesis, the block height from the metrics. Two subscriptions
stay open, to slot updates and, with `--tip-payment-program`, to the eight tip
accounts. Once a second the ledger reader asks, in one read where it can, for
the results of the transactions the fee payer card needs in the blocks rooted
since: every non-vote one of our blocks, and those of the payers it follows in
other leaders' blocks. `--rpc-url` moves all of it to another node.

#### Following the ledger

A secondary opens the validator's database read-only beside it. It takes no
lock and writes nothing into the ledger, but it keeps every file of the ledger
open, so a dashboard that stopped taking in the validator's writes would hold
the files the validator has purged since, and the ledger's disk would fill. Only
the dashboard's process ending frees them, so:

- a pass over the ledger that does not come back within 30 seconds ends the
  process;
- three passes that fail in a row, or another validator answering the admin
  socket, close the ledger and open it again;
- each second the follower tells systemd it is alive, so a unit with a watchdog
  ends a process that is stopped whole.

```ini
[Service]
ExecStart=/usr/local/bin/agave-dashboard --ledger /path/to/ledger ...
User=sol
Restart=always
WatchdogSec=60
NotifyAccess=main
LimitNOFILE=1000000
```

The secondary holds in memory what the validator has written and not yet
flushed to its own files, as the validator does.

Validator names are read with `getProgramAccounts` on the config program, once a
minute, and only where the validator indexes that program by owner (its command
line has `--account-index program-id`); unindexed, the call would read every
account. See [Validator names](#validator-names).

## Architecture

```
agave-validator -- ledger (RocksDB secondary) ----------------------------+
                -- metrics export (SOLANA_METRICS_CONFIG), line protocol --+
                -- JSON-RPC, RPC pubsub, admin socket ----------------------+
                -- /proc ----------------------------------------------------+
                                                                             v
agave-dashboard -- ledger follower (50 ms: first shreds, blocks, footers, roots)
                |- slot feed       (200 ms tick: slots, blocks, certificates, gossip)
                |- pollers         (admin socket, RPC, snapshots, names)
                |- ledger reader   (versions, fee payers)
                |- meters          (once a second: TPS, host, network, threads)
                +- server          (HTTP + websocket on one port)
```

The dashboard follows one validator at a time and starts over when another
answers its admin socket. The ledger follower sends the slot feed what the
ledger gains: each slot's first shred and parent, each block and footer once
its slot is whole and known frozen, and the confirmed and rooted slots. The
publisher retains the latest value of every key, so a client connecting late is
caught up in one shot.

The feed samples five times a second but publishes only what changed, so an
idle validator produces almost no websocket traffic. `server.rs` routes a
connection by peeking at its request head, then either serves an embedded asset
or hands the untouched socket to soketto.

The SPA lives in `frontend/`. Its `dist` directory is checked in so that a plain
`cargo build` needs no Node toolchain.

[INPUTS.md](INPUTS.md) lists every input the dashboard reads and what each one
feeds.

## Wire protocol

Every message is a JSON envelope:

```json
{ "topic": "summary", "key": "cluster", "value": "testnet" }
```

Topics currently published:

| Topic     | Keys |
|-----------|------|
| `summary` | `version`, `client`, `gossip_address`, `cluster`, `consensus`, `shred_version`, `identity_key`, `identity_name`, `identity_icon`, `vote_key`, `startup_time_nanos`, `server_time_nanos`, `uptime_nanos`, `startup_progress`, `gossip_stake`, `caught_up_time_nanos`, `root_slot`, `optimistically_confirmed_slot`, `finalized_slot`, `completed_slot`, `estimated_slot`, `behind_cluster`, `replay_rate`, `block_height`, `next_leader_slot`, `vote_slot`, `identity_balance`, `vote_balance`, `vote_cost`, `vote_commission`, `stake`, `validator_counts`, `versions`, `estimated_slot_duration_nanos`, `observed_slot_duration_nanos`, `finality_from_leader_micros`, `finalization_share`, `block_life`, `handoff`, `handoff_now`, `fee_payers`, `epoch_span`, `epoch_remaining_nanos`, `program_cache`, `accounts_cache`, `accounts_db`, `replay`, `shreds`, `slot_waterfalls`, `slot_costs`, `quic_paths`, `ingest_paths`, `verify`, `executed`, `produced_blocks`, `produced_block`, `produced_turns`, `bundles`, `tip_rates`, `skip_rate`, `leader_slot_counts`, `health`, `host`, `snapshots`, `tps_history`, `tps_sample`, `network`, `network_sample`, `net_interfaces`, `network_egress`, `xdp`, `turbine`, `threads_history`, `threads_sample`, `bls_key`, `admission`, `vote_credits`, `vote_participation`, `gossip` |
| `epoch`   | `new` |
| `peers`   | `all` |
| `slot`    | `overview`, `update` |

`summary.produced_blocks` is sent whole only to a client that is connecting;
after that, each of our blocks that is added or filled in arrives alone as
`summary.produced_block`.

`summary.network` totals the interfaces backed by a device, so a bond and its
members, or a tunnel and the link under it, count once. A port whose master is
also backed by a device is left out, and where no interface has one, every
interface but loopback counts. In `summary.net_interfaces`, `member_of` names,
on a port, its `master`, such as a bond, bridge or team, and `inside` names, on
a tunnel, the interface holding the IPv4 default route. Each interface's rates
come from the same read of `/proc/net/dev` as `summary.network`, every second,
so the two cover the same interval; its kind, state and routes are read every
five seconds.

A client can also send a request carrying an `id`, and the reply goes back to
that `id` alone: `summary.displays` for the whole name table,
`summary.misses` for the epoch's unrewarded votes one a row, `summary.written`
for what this node's own certificates left out per validator, both rebuilt
every five seconds while either is asked for and once a minute otherwise,
`epoch.query` for a held epoch's schedule, `slot.range` for a run of slots
out of the packed history, which holds an epoch, `slot.search` for a page of
the turns in it that match a leader's name or key, a slot number, this
validator or a condition on their slots or leaders, down or up from a given
slot or time, `slot.summary` for the same matches summed up overall and by
leader client, `slot.future` for a page of the epoch's turns to come whose
leader and slot match, all three answered off the async threads and one at a
time, and
`peers.gossip` for every node in gossip, which the server gathers only while it
is being asked for, `peers.leaders` for the client, version, address, country
and stake of every staked node and scheduled leader, where `peers.all` holds
only the leaders near the tip, `handoff.bin` for the settled leaders whose median handoff
gap falls in a range, from the figures each ten-second handoff summary keeps,
and `cluster.charts` for the Cluster page, gathered on the first ask and then
every fifteen seconds only while it is being asked for.

A client that offers the `deflate` websocket subprotocol is sent every message
of 512 bytes or more as a binary frame holding the zlib-deflated JSON, which the
page inflates with the browser's `DecompressionStream`; shorter messages, and
everything to a client that did not offer it, go as text. The kernel holds at
most 16 KB unsent for each websocket, so a client on a slow link falls behind in
the server's queue, and a burst from that queue carries each retained key and
each slot once, at its newest value. With the crate's log at debug, the server
reports the bytes published per key once a minute.

## Building the frontend

`frontend/dist` is committed to the repository, so a normal `cargo build` needs
no Node toolchain and produces a dashboard with the UI already inside it. You
only need the steps below when you change the UI, or when `dist` is missing:

```bash
cd dashboard/frontend
npm install
npm test               # pure logic: the bar scale, formatting, the store
npm run build          # writes dist/, which build.rs embeds
```

Then build the dashboard again and commit the regenerated `dist`. `build.rs`
watches the directory, so cargo picks the new bundle up on its own.

Because `dist` is committed, a reviewer is otherwise asked to accept a bundle
they cannot read. This rebuilds it into a scratch directory and compares the two
file by file:

```bash
npm run verify-dist
```

A mismatch usually means the bundle predates a source change, or was built
against different dependency versions. Run `npm run build` and commit the result.

`npm test` covers the logic that can be tested without a browser: the slot bar
scale, the formatters, the windowing the charts use, and the store's handling of
the message stream. Component rendering is not covered, which would mean pulling
in a DOM implementation and a testing library for the sake of it.

If `dist` is missing, the crate still builds. Cargo prints a warning, and the
server serves a page explaining how to produce the bundle rather than returning
a 404.

`npm run dev` serves the UI on Vite's dev server and proxies `/websocket` to a
dashboard on `127.0.0.1:11000`. This is the fast loop for UI work, since it needs
no Rust rebuild.

## Validator names

The slot strip and the schedule show validator names where it can find them,
and truncated pubkeys where it cannot.

Names live in accounts owned by the config program. Their addresses are not
derived from the identity they describe, because the tool that publishes one
generates a fresh keypair for it, so there is no address to compute from a
validator's pubkey and the only way to find these accounts is to search by
owner.

That search, RPC's `getProgramAccounts`, is affordable against the secondary
index and ruinous without it. Unindexed, the same call reads every account on
the validator off disk to check one field, which on a mainnet node means
hundreds of gigabytes and does not finish in any useful time. The dashboard
therefore reads the validator's command line for the index and skips the search
when it is absent, which is why an ordinary validator shows pubkeys. RPC must
serve the full API (`--full-rpc-api`).

To show names instead, start the validator with:

    --account-index program-id     --account-index-include-key Config1111111111111111111111111111111111111

The include key matters. On its own, `--account-index program-id` indexes every
program on the chain, which costs an entry per account at startup and on every
write thereafter. Restricted to the config program it costs two hash lookups per
account and stores a few thousand entries, which against the work index
generation already does per account is not measurable.

## IP geolocation

The Schedule and Gossip pages, the votes not rewarded list and a block's
left-out list show a country code beside each gossip address, the header shows
our own with the address behind it,
and Leader Handoff beside each leader's name, the Cluster page totals stake by
country and continent, and the finalization share is
compared with validators in our own country,
looked up in `geoip/country-ipv4.bin.gz`. That table is DB-IP's
[IP to Country Lite](https://db-ip.com/db/download/ip-to-country-lite),
November 2026 release, under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/); the page footer
carries the attribution DB-IP asks for. Only its form is changed: the IPv4 rows,
repacked by `geoip/generate.py`. IPv6 addresses get no code.

To refresh it:

    python3 geoip/generate.py YYYY-MM

## Not yet implemented

Two things have no equivalent here, because the data behind them does not exist
in Agave.

A shred timeline. `shred_fetch_stage` distinguishes turbine from repair via
`PacketFlags::REPAIR`, but nothing records per-shred arrival timing; the
blockstore keeps only each slot's first arrival, which is what the slot timing
reads. Recording it on the packet is not open to us: `solana-packet` is a
published crate rather than a workspace member, so `Meta` cannot gain a field.
The timing would have to be kept alongside and written from `modify_packets`,
which handles tens of thousands of shreds a second, so it would want
aggregating per slot rather than per shred.

Per-slot transaction detail. The fee, compute units and status of each
transaction are written by `TransactionStatusService`, which runs only when a
validator serves RPC with transaction history enabled, or a geyser plugin
requires it. A voting validator with no RPC records none of it, and turning the
flag on grows the blockstore substantially, which is not a trade worth making
for a panel. Without it the blockstore holds the entries and nothing more, so
the panel would come to a list of signatures beside counts this dashboard
already publishes. The blocks this validator produces are the exception in
part: the banking stage's own metrics give each of them its counts, fees,
tips and execution times, and the block page shows those, but still nothing
per transaction.
