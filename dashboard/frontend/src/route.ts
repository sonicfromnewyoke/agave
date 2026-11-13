/** Pages are paths, which the server answers with the page, and filters named keys:
 *  `/schedule?country=DE&duration=over-400`. */

import { useCallback, useLayoutEffect, useState, type MouseEvent } from "react";
import {
  conditionParams,
  decodeConditions,
  decodeValue,
  encodeValue,
  readConditionParams,
  type Condition,
} from "./scheduleConditions";
import { readOnly, type Only } from "./scheduleFilters";

export type Page = "overview" | "slots" | "schedule" | "gossip" | "cluster" | "accounts";

const PAGES: Page[] = ["overview", "slots", "schedule", "gossip", "cluster", "accounts"];

export interface Route {
  page: Page;
  slot: number | null;
  query: string;
  ours: boolean;
  /** The schedule's jump, as typed. */
  from: string;
  /** The schedule's filters. */
  only: readonly Only[];
  /** The schedule's range, each end as typed. */
  since: string;
  until: string;
  /** The schedule's column conditions. */
  where: readonly Condition[];
}

export const HOME: Route = {
  page: "overview",
  slot: null,
  query: "",
  ours: false,
  from: "",
  only: [],
  since: "",
  until: "",
  where: [],
};

/** An address's query as keys and undecoded values, a bare key holding "". */
function readParams(search: string): (readonly [key: string, raw: string])[] {
  return search
    .split("&")
    .filter(Boolean)
    .map((part) => {
      const at = part.indexOf("=");
      return at === -1 ? ([part, ""] as const) : ([part.slice(0, at), part.slice(at + 1)] as const);
    });
}

/** A route from an address, its path and query. */
export function readRoute(address: string): Route {
  const at = address.indexOf("?");
  const [path, search] = at === -1 ? [address, ""] : [address.slice(0, at), address.slice(at + 1)];
  const [name, rest] = path.replace(/^\//, "").split("/");
  const page = PAGES.find((page) => page === name) ?? "overview";
  const params = readParams(search);
  const raw = (key: string): string | null => params.find(([held]) => held === key)?.[1] ?? null;
  const text = (key: string): string => decodeValue(raw(key) ?? "");
  return {
    page,
    slot: page === "slots" && rest !== undefined && /^\d+$/.test(rest) ? Number(rest) : null,
    // Only these pages keep their search in the address.
    query: page === "slots" || page === "schedule" ? text("q") : "",
    ours: page === "schedule" && raw("ours") !== null,
    from: page === "schedule" ? text("from") : "",
    only: page === "schedule" ? readOnly(text("only")) : [],
    since: page === "schedule" ? text("since") : "",
    until: page === "schedule" ? text("until") : "",
    where: page === "schedule" ? [...decodeConditions(text("where")), ...readConditionParams(params)] : [],
  };
}

/** The address an old `#/schedule?where=…` link or saved view now means. */
export function addressOfHash(hash: string): string {
  return routeAddress(readRoute(`/${hash.replace(/^#\/?/, "")}`));
}

export function routeAddress(route: Route): string {
  switch (route.page) {
    case "overview":
      return "/";
    case "slots":
      return withSearch(route.slot === null ? "/slots" : `/slots/${route.slot}`, { q: route.query });
    case "schedule":
      return withSearch(
        "/schedule",
        {
          q: route.query,
          ours: route.ours,
          from: route.from,
          only: route.only.join(","),
          since: route.since,
          until: route.until,
        },
        conditionParams(route.where),
      );
    case "gossip":
      return "/gossip";
    case "cluster":
      return "/cluster";
    case "accounts":
      return "/accounts";
  }
}

/** The schedule landed on a slot. */
export function slotAddress(slot: number): string {
  return routeAddress({ ...HOME, page: "schedule", from: String(slot) });
}

/** Empty and false values are left out; true is written as a bare key. */
function withSearch(path: string, values: Record<string, string | boolean>, more: readonly string[] = []): string {
  const parts = Object.entries(values).flatMap(([key, value]) =>
    value === true ? [key] : value ? [`${key}=${encodeValue(value)}`] : [],
  );
  const search = [...parts, ...more].join("&");
  return search ? `${path}?${search}` : path;
}

/** A click the browser should not handle itself: not one opening a new tab or window. */
export function plainClick(event: MouseEvent): boolean {
  return event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey;
}

/** The address now held, an old hash link rewritten to its path. */
function settle(): Route {
  const { hash, pathname, search } = window.location;
  if (!hash) return readRoute(`${pathname}${search}`);
  const address = addressOfHash(hash);
  window.history.replaceState(null, "", address);
  return readRoute(address);
}

/** A page change is a history entry and anything else replaces it, so back steps between pages. */
function write(address: string, replace: boolean): void {
  const { hash, pathname, search } = window.location;
  if (replace || address === `${pathname}${search}${hash}`) window.history.replaceState(null, "", address);
  else window.history.pushState(null, "", address);
}

/** Goes to an address from outside the route's owner, as a link in the page does. */
export function openAddress(address: string, replace = false): void {
  write(address, replace);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

export function useRoute(): [Route, (next: Route, replace?: boolean) => void] {
  const [route, setRoute] = useState<Route>(settle);

  // Ahead of the pages' effects, one of which can open a saved view on arriving.
  useLayoutEffect(() => {
    const follow = () => setRoute(settle());
    window.addEventListener("popstate", follow);
    // An old link pasted over this page changes only the hash.
    window.addEventListener("hashchange", follow);
    return () => {
      window.removeEventListener("popstate", follow);
      window.removeEventListener("hashchange", follow);
    };
  }, []);

  const go = useCallback((next: Route, replace = false) => {
    write(routeAddress(next), replace);
    setRoute(next);
  }, []);

  return [route, go];
}
