import { useEffect, useState, type ReactElement } from "react";
import { EpochCard, ClusterCard, TransactionsCard } from "./components/cards";
import { Header } from "./components/Header";
import { CachesCard } from "./components/CachesCard";
import { HostCard } from "./components/HostCard";
import { IngestCard } from "./components/IngestCard";
import { NetworkCard } from "./components/NetworkCard";
import { BlockLifeCard } from "./components/BlockLifeCard";
import { ReplayCard } from "./components/ReplayCard";
import { SchedulePage } from "./components/SchedulePage";
import { SlotDetailsPage } from "./components/SlotDetailsPage";
import { StartupCard } from "./components/Verdict";
import { TpuPathCard } from "./components/TpuPathCard";
import { GossipPage } from "./components/GossipPage";
import { ClusterPage } from "./components/ClusterPage";
import { AccountsPage } from "./components/AccountsPage";
import { GossipStakeCard } from "./components/GossipStakeCard";
import { MissesPanel } from "./components/MissesPanel";
import { SlotStrip } from "./components/SlotStrip";
import { HOME, plainClick, routeAddress, useRoute, type Page } from "./route";
import { useStore } from "./useStore";
import { pageTitle } from "./title";

export function App(): ReactElement {
  const store = useStore();
  const connection = store.getConnection();
  const name = store.get("summary", "identity_name");
  const identity = store.get("summary", "identity_key");
  const cluster = store.get("summary", "cluster");
  const [route, go] = useRoute();
  const page = route.page;

  useEffect(() => {
    document.title = pageTitle(name, identity, cluster);
  }, [name, identity, cluster]);

  const classes = ["app"];
  // While the validator boots, everything but the boot sequence is blurred, by the same test the
  // verdict uses.
  const startup = store.get("summary", "startup_progress");
  if (startup && !startup.running) classes.push("is-booting");

  return (
    <div className={classes.join(" ")}>
      <main className="main">
        <Header
          folds={page === "overview"}
          nav={<Nav page={page} onSelect={(next) => go({ ...HOME, page: next })} />}
        />
        {connection === "closed" && (
          <div className="banner">
            Disconnected from the validator. Retrying…
          </div>
        )}
        {page === "overview" && <Overview />}
        {page === "slots" && (
          <SlotDetailsPage
            slot={route.slot}
            query={route.query}
            onSlot={(slot) => go({ ...route, slot }, true)}
            onQuery={(query) => go({ ...route, query }, true)}
          />
        )}
        {page === "schedule" && (
          <SchedulePage
            query={route.query}
            ours={route.ours}
            from={route.from}
            only={route.only}
            since={route.since}
            until={route.until}
            where={route.where}
            onFilter={(next) => go({ ...route, ...next }, true)}
          />
        )}
        {page === "cluster" && <ClusterPage />}
        {page === "gossip" && <GossipPage />}
        {page === "accounts" && <AccountsPage />}
        <footer className="footer">
          <a href="https://db-ip.com" target="_blank" rel="noreferrer">
            IP Geolocation by DB-IP
          </a>
        </footer>
      </main>
    </div>
  );
}

function Overview() {
  // Open for this visit only: a list of a hundred rows is not a place to
  // come back to on a reload.
  const [missesOpen, setMissesOpen] = useState(false);
  return (
    <>
      <StartupCard />
      <SlotStrip />
      <div className="grid is-three">
        <EpochCard missesOpen={missesOpen} onToggleMisses={() => setMissesOpen((was) => !was)} />
        <ClusterCard />
        <TransactionsCard />
      </div>
      {missesOpen && <MissesPanel onClose={() => setMissesOpen(false)} />}
      <GossipStakeCard />
      <div className="grid">
        <NetworkCard />
        <IngestCard />
      </div>
      <HostCard />
      <BlockLifeCard />
      <ReplayCard />
      <CachesCard />
      <TpuPathCard />
    </>
  );
}

/** `short` is the label on a phone, where the tabs do not fit at full length. */
const PAGES: { page: Page; label: string; short?: string }[] = [
  { page: "overview", label: "Overview" },
  { page: "slots", label: "Slot details", short: "Slots" },
  { page: "schedule", label: "Schedule" },
  { page: "gossip", label: "Gossip" },
  { page: "cluster", label: "Cluster" },
  { page: "accounts", label: "Accounts" },
];

/** Anchors rather than buttons, so a page can open in a new tab; the click is
 *  still handled to avoid a reload. */
function Nav({ page, onSelect }: { page: Page; onSelect: (page: Page) => void }) {
  return (
    <nav className="nav" aria-label="Pages">
      {PAGES.map((entry) => (
        <a
          key={entry.page}
          href={routeAddress({ ...HOME, page: entry.page })}
          className={`nav-tab${page === entry.page ? " is-current" : ""}`}
          aria-current={page === entry.page ? "page" : undefined}
          onClick={(event) => {
            if (!plainClick(event)) return;
            event.preventDefault();
            onSelect(entry.page);
          }}
        >
          {entry.short ? (
            <>
              <span className="nav-long">{entry.label}</span>
              <span className="nav-short">{entry.short}</span>
            </>
          ) : (
            entry.label
          )}
        </a>
      ))}
    </nav>
  );
}
