import { describe, expect, it } from "vitest";
import { addressOfHash, HOME, plainClick, readRoute, routeAddress, slotAddress, type Route } from "./route";

describe("slotAddress", () => {
  it("lands the schedule on the slot, in plain digits", () => {
    expect(slotAddress(465584886)).toBe("/schedule?from=465584886");
    expect(readRoute(slotAddress(465584886))).toEqual({ ...HOME, page: "schedule", from: "465584886" });
  });
});

describe("readRoute", () => {
  it("reads a page out of the path", () => {
    expect(readRoute("/schedule").page).toBe("schedule");
    expect(readRoute("/schedule/").page).toBe("schedule");
    expect(readRoute("/slots").page).toBe("slots");
    expect(readRoute("/gossip").page).toBe("gossip");
    expect(readRoute("/cluster").page).toBe("cluster");
    expect(readRoute("/accounts").page).toBe("accounts");
  });

  it("falls back to the overview rather than showing nothing", () => {
    expect(readRoute("")).toEqual(HOME);
    expect(readRoute("/")).toEqual(HOME);
    expect(readRoute("/nonsense")).toEqual(HOME);
  });

  it("reads an open block on the slot page", () => {
    expect(readRoute("/slots/5539826")).toEqual({ ...HOME, page: "slots", slot: 5_539_826 });
    expect(readRoute("/slots/abc").slot).toBeNull();
    expect(readRoute("/schedule/5539826").slot).toBeNull();
  });

  it("reads the schedule's filter", () => {
    expect(readRoute("/schedule?q=Mithril+Alpenglow&ours=")).toEqual({
      page: "schedule",
      slot: null,
      query: "Mithril Alpenglow",
      ours: true,
      from: "",
      only: [],
      since: "",
      until: "",
      where: [],
    });
    expect(readRoute("/schedule?from=08%3A00&only=slow,left_out")).toEqual({
      ...HOME,
      page: "schedule",
      from: "08:00",
      only: ["slow", "left_out"],
    });
    expect(readRoute("/gossip?from=1&only=slow&country=DE")).toEqual({ ...HOME, page: "gossip" });
    expect(readRoute("/schedule?ours").ours).toBe(true);
    expect(readRoute("/slots?q=826&ours")).toEqual({ ...HOME, page: "slots", query: "826" });
    expect(readRoute("/slots/5539826?q=826").slot).toBe(5_539_826);
    expect(readRoute("/?q=x").query).toBe("");
    expect(readRoute("/gossip?q=Jito&ours")).toEqual({ ...HOME, page: "gossip" });
    expect(readRoute("/cluster?q=x")).toEqual({ ...HOME, page: "cluster" });
    expect(readRoute("/schedule?q=100%").query).toBe("100%");
  });

  it("reads conditions from their keys, in order", () => {
    expect(readRoute("/schedule?country=DE,NL&duration=over-400&duration=under-900&not-client=Agave").where).toEqual([
      { field: "country", op: "is", value: null, values: ["DE", "NL"] },
      { field: "duration", op: "gt", value: 400, values: [] },
      { field: "duration", op: "lt", value: 900, values: [] },
      { field: "client", op: "is_not", value: null, values: ["Agave"] },
    ]);
  });
});

describe("routeAddress", () => {
  it("round-trips every route", () => {
    const routes: Route[] = [
      HOME,
      { ...HOME, page: "slots" },
      { ...HOME, page: "slots", slot: 12 },
      { ...HOME, page: "slots", slot: 12, query: "1 2" },
      { ...HOME, page: "slots", query: "9+1" },
      { ...HOME, page: "schedule" },
      { ...HOME, page: "schedule", query: "a b&c", ours: true },
      { ...HOME, page: "schedule", ours: true },
      { ...HOME, page: "schedule", from: "Nov 2 08:00", only: ["skipped", "no_tips"] },
      {
        ...HOME,
        page: "schedule",
        since: "Nov 2 08:00",
        until: "464,000,000",
        where: [
          { field: "base_fee", op: "gt", value: 0.0025, values: [] },
          { field: "compute", op: "ge", value: 40_000_000, values: [] },
          { field: "gap", op: "lt", value: 5400, values: [] },
          { field: "shreds", op: "le", value: 0, values: [] },
          { field: "client", op: "is", value: null, values: ["Agave Bam", "x|y"] },
          { field: "name", op: "is_not", value: null, values: ["Hamsa, Labs", "not-me", "a+b=c&d"] },
        ],
      },
      { ...HOME, page: "gossip" },
      { ...HOME, page: "cluster" },
      { ...HOME, page: "accounts" },
    ];
    for (const route of routes) expect(readRoute(routeAddress(route))).toEqual(route);
  });

  it("names nothing it does not need to", () => {
    expect(routeAddress(HOME)).toBe("/");
    expect(routeAddress({ ...HOME, page: "slots" })).toBe("/slots");
    expect(routeAddress({ ...HOME, page: "schedule" })).toBe("/schedule");
  });

  it("writes filters to be read", () => {
    expect(
      routeAddress({
        ...HOME,
        page: "schedule",
        query: "Coinbase 05",
        ours: true,
        from: "460,049,249",
        since: "Nov 2 08:00",
        where: [
          { field: "country", op: "is", value: null, values: ["DE"] },
          { field: "duration", op: "gt", value: 400, values: [] },
          { field: "name", op: "is_not", value: null, values: ["Hamsa, Labs"] },
        ],
      }),
    ).toBe("/schedule?q=Coinbase+05&ours&from=460,049,249&since=Nov+2+08:00&country=DE&duration=over-400&not-name=Hamsa%2C+Labs");
  });
});

describe("addressOfHash", () => {
  it("moves an old link to its path", () => {
    expect(addressOfHash("#")).toBe("/");
    expect(addressOfHash("#/slots/12?q=1")).toBe("/slots/12?q=1");
    expect(addressOfHash("#/schedule?where=country%3Ais%3ADE%3Bduration%3Agt%3A400")).toBe(
      "/schedule?country=DE&duration=over-400",
    );
    expect(addressOfHash("#/schedule?ours=&only=slow&where=client%3Ais_not%3AAgave%2520Bam%7CJitoLabs")).toBe(
      "/schedule?ours&only=slow&not-client=Agave+Bam,JitoLabs",
    );
  });
});

describe("plainClick", () => {
  const click = (fill: Partial<MouseEvent>) =>
    ({ button: 0, metaKey: false, ctrlKey: false, shiftKey: false, altKey: false, ...fill }) as never;

  it("leaves new tabs and windows to the browser", () => {
    expect(plainClick(click({}))).toBe(true);
    expect(plainClick(click({ ctrlKey: true }))).toBe(false);
    expect(plainClick(click({ metaKey: true }))).toBe(false);
    expect(plainClick(click({ shiftKey: true }))).toBe(false);
    expect(plainClick(click({ button: 1 }))).toBe(false);
  });
});
