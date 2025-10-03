/* app/api/pull-lists/run/route.ts
   Next.js App Router (v13+ / v15). Streams progress lines and NDJSON for CSVs.

   POST body JSON:
   {
     token: string,
     zips: string[],
     tags?: string[],                // currently unused
     importToHighLevel?: boolean     // currently unused
   }
*/

export const runtime = "nodejs";
export const dynamic = "force-dynamic"; // ensure no static optimization in dev

// ========= Types =========

type RunInput = {
  token: string;
  zips: string[];
  tags?: string[];
  importToHighLevel?: boolean;
};

type LeadsCountResponse = {
  valid?: boolean;
  error?: unknown;
  results?: {
    total_lead_count?: number;
  };
};

type DMContact = {
  given_name?: string | null;
  middle_initial?: string | null;
  surname?: string | null;
  full_name?: string | null;

  // plaintext phones + types
  phone_1?: string | null;
  phone_1_type?: string | null;
  phone_2?: string | null;
  phone_2_type?: string | null;
  phone_3?: string | null;
  phone_3_type?: string | null;

  // plaintext emails
  email_address_1?: string | null;
  email_address_2?: string | null;
  email_address_3?: string | null;

  // possible hints to ownership
  is_owner?: boolean;
  is_primary?: boolean;
  primary?: boolean;
  homeowner?: boolean;
  owner?: boolean;
  role?: string | null;
  type?: string | null;
  relationship?: string | null;
  contact_type?: string | null;
};

type PhoneNumberEntry = {
  contact?: DMContact | null;
  // Sometimes flags/strings can live on the entry itself
  is_owner?: boolean;
  is_primary?: boolean;
  homeowner?: boolean;
  role?: string | null;
  type?: string | null;
  relationship?: string | null;
};

type DMProperty = {
  property_address_full?: string | null;
  phone_numbers?: PhoneNumberEntry[] | null;
};

type LeadsPageResponse = {
  error?: unknown;
  results?: {
    properties?: DMProperty[];
  };
};

type HomeownerRow = {
  firstName: string;
  lastName: string;
  propertyAddress: string;
  addy_two: string;                  // NEW: street-only (no city/state/zip)
  mobile: string;                    // the first phone whose type === "W"
  phones: { number: string; type: string }[];
  emails: string[];
};

// =========================
// ======== CONFIG =========
const LEADS_PER_PAGE = 100;
const MAX_CONCURRENCY = 50;           // safer default for server
const POLL_INTERVAL_MS = 5_000;
const POLL_BUILD_TIMEOUT_MS = 60 * 60 * 1000;   // 60 minutes
const POLL_DELETE_TIMEOUT_MS = 30 * 60 * 1000;  // 30 minutes

const JSON_HEADERS: HeadersInit = {
  accept: "application/json",
  "content-type": "application/json",
};

// =========================
// ====== HTTP UTILS =======
async function httpPost<T = unknown>(url: string, bodyObj: unknown): Promise<T> {
  const res = await fetch(url, {
    method: "POST",
    headers: JSON_HEADERS,
    body: JSON.stringify(bodyObj),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status} ${res.statusText} at ${url} :: ${text}`);
  }
  return (await res.json()) as T;
}
function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// ==========================
// ===== STREAM HELPERS =====
const te = new TextEncoder();
function line(controller: ReadableStreamDefaultController<Uint8Array>, s: string) {
  controller.enqueue(te.encode(s + "\n"));
}
function jsonLine(controller: ReadableStreamDefaultController<Uint8Array>, obj: unknown) {
  controller.enqueue(te.encode(JSON.stringify(obj) + "\n"));
}

// =========================
// ===== CORE CALLS =========
async function getTotalLeadCount(token: string): Promise<number> {
  const body = {
    token,
    type: "count",
    search: "",
    search_type: "address",
    filters: null,
    old_filters: null,
    list_id: "all_leads",
    property_flags: "",
    property_flags_and_or: "or",
    get_updated_data: false,
    list_history_id: null,
  };
  const json = await httpPost<LeadsCountResponse>("https://api.dealmachine.com/v2/leads/", body);
  if (json?.error || !json?.valid) {
    throw new Error(`Count error: ${JSON.stringify(json)}`);
  }
  const n = json?.results?.total_lead_count;
  if (typeof n !== "number") {
    throw new Error(`Count missing total_lead_count. Raw: ${JSON.stringify(json)}`);
  }
  return n;
}

async function pollUntilCountEquals(
  token: string,
  target: number,
  timeoutMs: number,
  label: string,
  controller: ReadableStreamDefaultController<Uint8Array>
) {
  const start = Date.now();
  while (true) {
    const n = await getTotalLeadCount(token);
    line(controller, `[Poll ${label}] current=${n}, target=${target}`);
    if (n === target) return true;
    if (Date.now() - start > timeoutMs) return false;
    await sleep(POLL_INTERVAL_MS);
  }
}

async function issueSingleDeleteAllExact(token: string, currentCount: number) {
  const url = "https://api.dealmachine.com/v2/bulk-update-leads/";
  const body = {
    token,
    type: "permanently_delete",
    select_all: 1,
    total_count: Number.isFinite(currentCount) ? currentCount : 0,
    lead_ids: "",
    new_list_name: null,
    new_tag_name: null,
    accept_new_owner: 0,
    list_history_id: "",
    list_id: "all_leads",
    search: "",
    search_type: "address",
    property_flags: "",
    property_flags_and_or: "or",
    filters: null,
  };
  const res = await fetch(url, {
    method: "POST",
    headers: JSON_HEADERS,
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status} ${res.statusText}\n${text}`);
  }
  const data = (await res.json()) as unknown;
  return data;
}

async function buildListForZip(token: string, zip: string) {
  const search_locations = JSON.stringify([{ type: "zip", value: String(zip) }]);
  const body = {
    token,
    title: "My List",
    type: "build_list_v2",
    using_new_filters: 1,
    list_type: "build_list",
    list_area_type: "zip",
    list_area: null,
    list_area_2: null,
    list_geo_fence: null,
    list_filters: null,
    estimated_count: null,
    property_flags: "",
    property_types: "",
    property_flags_and_or: "or",
    value_range_min: "",
    value_range_max: "",
    price_type: "estimated_value",
    beds_min: null,
    baths_min: null,
    use_beds_exact: false,
    search_locations,
    prompt: null,
    variance: null,
    attached_property_ids: null,
    use_vision: false,
  };

  const json = await httpPost<unknown>("https://api.dealmachine.com/v2/list-builder/", body);
  // minimal defensive access:
  const anyJson = json as Record<string, unknown>;
  if ((anyJson as { error?: unknown }).error) {
    throw new Error(`List Builder error for ZIP ${zip}: ${JSON.stringify(json)}`);
  }
  const results = (anyJson as { results?: Record<string, unknown> }).results ?? {};
  const buildCount =
    (results as { build_count?: number }).build_count ??
    (results as { estimated_count?: number }).estimated_count ??
    null;

  if (buildCount == null) {
    throw new Error(`No build_count/estimated_count for ZIP ${zip}. Raw: ${JSON.stringify(json)}`);
  }
  return { buildCount };
}

async function fetchLeadsPage(token: string, begin = 0) {
  const body = {
    token,
    sort_by: "date_created_desc",
    limit: LEADS_PER_PAGE,
    begin,
    search: "",
    search_type: "phone",
    filters: null,
    old_filters: null,
    list_id: "all_leads",
    property_flags: "",
    property_flags_and_or: "or",
    get_updated_data: false,
    list_history_id: null,
  };
  const json = await httpPost<LeadsPageResponse>("https://api.dealmachine.com/v2/leads/", body);
  return json;
}

// ===============================
// ====== HOMEOWNER PARSING ======
//
// NEW behavior:
//   - Pick ONLY the homeowner contact for each property (not every household member)
//   - Add "mobile" column: the FIRST phone whose type === "W" (per your mapping)
//   - Drop rows with NO mobile
//   - De-duplicate rows later (address+mobile)
//   - FirstName cased to only the first letter upper, rest lower
//   - New column addy_two = street portion before first comma

function normalizePhone(num: string | number | null | undefined): string {
  if (num == null) return "";
  const s = String(num).trim();
  if (!s) return "";
  const hasPlus = s.startsWith("+");
  const digits = s.replace(/\D+/g, "");
  return hasPlus ? `+${digits}` : digits;
}

function contactFirstLast(c: DMContact): { first: string; last: string } {
  let first = (c?.given_name ?? "").toString().trim();
  let last = (c?.surname ?? "").toString().trim();

  if (!first && !last) {
    const full = (c?.full_name ?? "").toString().trim();
    if (full) {
      const parts = full.split(/\s+/).filter(Boolean);
      if (parts.length === 1) {
        first = parts[0];
      } else if (parts.length > 1) {
        first = parts[0];
        last = parts[parts.length - 1];
      }
    }
  }
  return { first, last };
}

function capitalizeFirstOnly(s: string): string {
  if (!s) return s;
  const lower = s.toLowerCase();
  return lower.charAt(0).toUpperCase() + lower.slice(1);
}

/** Best-effort heuristic to detect the homeowner contact from a phone_numbers entry. */
function isHomeownerContact(entry: PhoneNumberEntry): boolean {
  const c = entry?.contact ?? {};
  const flags = [
    Boolean(c?.is_owner),
    Boolean(c?.is_primary),
    Boolean(c?.primary),
    Boolean(c?.homeowner),
    Boolean(c?.owner),
    Boolean(entry?.is_owner),
    Boolean(entry?.is_primary),
    Boolean(entry?.homeowner),
  ];

  const strings = [
    c?.role,
    c?.type,
    c?.relationship,
    c?.contact_type,
    entry?.role,
    entry?.type,
    entry?.relationship,
  ]
    .filter((v): v is string => typeof v === "string" && v.length > 0)
    .map((s) => s.toLowerCase());

  const anyOwnerWord = strings.some((s) => s.includes("owner") || s.includes("homeowner"));
  const anyFlag = flags.some(Boolean);

  return anyOwnerWord || anyFlag;
}

/** Pick the best homeowner entry (fallback to first). */
function selectHomeownerEntry(property: DMProperty): PhoneNumberEntry | null {
  const list = Array.isArray(property?.phone_numbers) ? property.phone_numbers : [];
  if (list.length === 0) return null;
  const found = list.find((e) => isHomeownerContact(e));
  return found ?? list[0];
}

/** Parse ONLY the homeowner per property. Drop if no "W" (mobile) phone. */
function parseHomeownersFromPage(rawPage: LeadsPageResponse): HomeownerRow[] {
  const props = rawPage?.results?.properties;
  if (!Array.isArray(props)) return [];

  const rows: HomeownerRow[] = [];

  for (const property of props) {
    const propertyAddress = (property?.property_address_full ?? "") || "";
    const addy_two = propertyAddress.split(",")[0]?.trim() ?? ""; // NEW: street only
    const entry = selectHomeownerEntry(property);
    if (!entry) continue;

    const c = entry.contact;
    if (!c) continue;

    // Build phones set (from homeowner contact only)
    const phoneTriples: Array<{ num: string | null | undefined; typ: string | null | undefined }> = [
      { num: c.phone_1 ?? null, typ: c.phone_1_type ?? null },
      { num: c.phone_2 ?? null, typ: c.phone_2_type ?? null },
      { num: c.phone_3 ?? null, typ: c.phone_3_type ?? null },
    ];

    const seen = new Set<string>();
    const phones: { number: string; type: string }[] = [];
    let mobile = "";

    for (const p of phoneTriples) {
      const n = normalizePhone(p.num);
      const t = (p?.typ ?? "").toString().trim();
      if (!n || seen.has(n)) continue;
      seen.add(n);
      phones.push({ number: n, type: t });

      // Per your rule: "W" means mobile; pick the first one
      if (!mobile && t.toUpperCase() === "W") {
        mobile = n;
      }
    }

    // If NO mobile, drop this homeowner completely
    if (!mobile) continue;

    // Emails (from homeowner contact only)
    const emails = [c.email_address_1, c.email_address_2, c.email_address_3]
      .map((e) => (e ?? "").toString().trim())
      .filter((e) => e.length > 0);

    const { first, last } = contactFirstLast(c);
    const firstNameCased = capitalizeFirstOnly(first || "");

    rows.push({
      firstName: firstNameCased,  // NEW: first letter upper, rest lower
      lastName: last || "",
      propertyAddress,
      addy_two,                   // NEW column value
      mobile,                     // NEW column already present
      phones,
      emails,
    });
  }

  return rows;
}

// ===============================
// ===== CSV HELPERS (updated) ===
//
// Add "addy_two" column after propertyAddress; keep "mobile" column; keep dynamic phones/emails.
function escapeCsvValue(value: unknown) {
  const s = String(value ?? "");
  if (s.includes(",") || s.includes('"') || s.includes("\n")) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}
function generateCsvHeaders(maxPhones: number, maxEmails: number) {
  const base = ["firstName", "lastName", "propertyAddress", "addy_two", "mobile"]; // NEW: addy_two inserted
  const phoneHeaders: string[] = [];
  for (let i = 1; i <= maxPhones; i++) {
    phoneHeaders.push(`phone${i}`, `phone${i}_type`);
  }
  const emailHeaders: string[] = [];
  for (let i = 1; i <= maxEmails; i++) {
    emailHeaders.push(`email${i}`);
  }
  return [...base, ...phoneHeaders, ...emailHeaders].join(",");
}
function convertRowToCsv(row: HomeownerRow, maxPhones: number, maxEmails: number) {
  const fields: Array<string> = [
    row.firstName,
    row.lastName,
    row.propertyAddress,
    row.addy_two,  // NEW
    row.mobile,
  ];

  for (let i = 0; i < maxPhones; i++) {
    const p = row.phones[i];
    fields.push(p?.number ?? "", p?.type ?? "");
  }
  for (let i = 0; i < maxEmails; i++) {
    const e = row.emails[i];
    fields.push(e ?? "");
  }
  return fields.map(escapeCsvValue).join(",");
}

// ===========================================
// ======= EXPORT (homeowner-only) ===========
async function exportHomeownersCsvString(
  token: string,
  controller: ReadableStreamDefaultController<Uint8Array>,
  zip: string
): Promise<string> {
  const total = await getTotalLeadCount(token);
  if (total <= 0) {
    line(controller, `[${zip}] Nothing to export (0 leads).`);
    return "firstName,lastName,propertyAddress,addy_two,mobile\n"; // empty with headers (NEW header order)
  }

  const pages = Math.ceil(total / LEADS_PER_PAGE);
  line(
    controller,
    `[${zip}] Exporting ${total} leads across ${pages} page(s) with concurrency=${Math.min(
      MAX_CONCURRENCY,
      pages
    )} …`
  );

  const begins = Array.from({ length: pages }, (_, i) => i * LEADS_PER_PAGE);
  const results: HomeownerRow[][] = [];
  let idx = 0;

  async function worker() {
    while (idx < begins.length) {
      const myIndex = idx++;
      const begin = begins[myIndex];
      try {
        const raw = await fetchLeadsPage(token, begin);
        // homeowner-only + drop rows without mobile
        const parsed = parseHomeownersFromPage(raw);
        results[myIndex] = parsed;
        line(
          controller,
          `[${zip}] Page ${myIndex + 1}/${pages} parsed ${parsed.length} homeowner row(s).`
        );
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        line(controller, `[${zip}] Error fetching page at begin=${begin}: ${msg}`);
        results[myIndex] = [];
      }
    }
  }

  const workers = Array.from(
    { length: Math.min(MAX_CONCURRENCY, begins.length) },
    () => worker()
  );
  await Promise.all(workers);

  // Flatten
  const allRows = results.flat();

  // ====== NEW: De-duplication ======
  // Use a stable key of propertyAddress (case-insensitive, trimmed) + mobile.
  const seen = new Set<string>();
  const deduped: HomeownerRow[] = [];
  for (const r of allRows) {
    const key =
      `${r.propertyAddress.trim().toLowerCase()}|${r.mobile.replace(/\D+/g, "")}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(r);
  }

  // Compute dynamic widths from deduped rows
  let maxPhones = 0;
  let maxEmails = 0;
  for (const r of deduped) {
    if (r.phones.length > maxPhones) maxPhones = r.phones.length;
    if (r.emails.length > maxEmails) maxEmails = r.emails.length;
  }

  const header = generateCsvHeaders(maxPhones, maxEmails);
  let csv = header + "\n";
  for (const row of deduped) {
    csv += convertRowToCsv(row, maxPhones, maxEmails) + "\n";
  }
  line(
    controller,
    `[${zip}] CSV built with ${deduped.length} homeowner row(s) after de-duplication (phone pairs: ${maxPhones}, email cols: ${maxEmails}).`
  );
  return csv;
}

// ===========================================
// ================ HANDLERS =================
export async function GET() {
  return new Response("OK /api/pull-lists/run (POST expected)", {
    status: 200,
    headers: { "content-type": "text/plain" },
  });
}

export async function POST(req: Request) {
  let input: RunInput | null = null;
  try {
    input = (await req.json()) as RunInput;
  } catch {
    return new Response("Invalid JSON body", { status: 400 });
  }

  if (!input || typeof input.token !== "string" || !Array.isArray(input.zips)) {
    return new Response("Missing token or zips[]", { status: 400 });
  }

  const token = input.token.trim();
  const zips = input.zips.map((z) => String(z).trim()).filter((z) => /^\d{5}$/.test(z));

  if (!token) return new Response("Empty token", { status: 400 });
  if (zips.length === 0) return new Response("No valid zips provided", { status: 400 });

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      try {
        jsonLine(controller, { type: "phase", message: "Starting run…" });

        // ===== Phase 0: Pre-delete any existing leads =====
        line(controller, "Phase 0 — Detecting existing leads…");
        let current = await getTotalLeadCount(token);
        line(controller, `Leads already detected: ${current}`);

        if (current > 0) {
          line(controller, "Deleting existing leads…");
          await issueSingleDeleteAllExact(token, current);
          const okDel = await pollUntilCountEquals(
            token,
            0,
            POLL_DELETE_TIMEOUT_MS,
            "delete:init",
            controller
          );
          if (!okDel) throw new Error("Timed out waiting for initial delete to finish (count->0).");
          line(controller, "Initial delete complete (count=0).");
        } else {
          line(controller, "No existing leads, nothing to delete.");
        }

        // ===== Per-ZIP phases =====
        for (const zip of zips) {
          jsonLine(controller, { type: "phase", message: `\n— ZIP ${zip} —` });

          // Build
          line(controller, `[${zip}] Building list…`);
          const { buildCount } = await buildListForZip(token, zip);
          line(controller, `[${zip}] Build count (expected): ${buildCount}`);

          line(controller, `[${zip}] Polling count until it equals ${buildCount} …`);
          const okBuild = await pollUntilCountEquals(
            token,
            buildCount,
            POLL_BUILD_TIMEOUT_MS,
            `build:${zip}`,
            controller
          );
          if (!okBuild) throw new Error(`[${zip}] Timed out waiting for build to reach ${buildCount}.`);
          line(controller, `[${zip}] Build complete.`);

          // Export CSV (HOMEOWNER-ONLY, must have mobile "W"; with de-dup + addy_two + firstName casing)
          line(controller, `[${zip}] Starting export (homeowner-only)…`);
          const csv = await exportHomeownersCsvString(token, controller, zip);

          // Send CSV back as NDJSON line (base64)
          const b64 = Buffer.from(csv, "utf8").toString("base64");
          jsonLine(controller, {
            type: "csv",
            zip,
            filename: `${zip}.csv`,
            dataBase64: b64,
          });

          // Delete all after export (cleanup)
          line(controller, `[${zip}] Deleting all leads (cleanup)…`);
          current = await getTotalLeadCount(token);
          await issueSingleDeleteAllExact(token, current);
          const okDel2 = await pollUntilCountEquals(
            token,
            0,
            POLL_DELETE_TIMEOUT_MS,
            `delete:${zip}`,
            controller
          );
          if (!okDel2) throw new Error(`[${zip}] Timed out waiting for delete to finish (count->0).`);
          line(controller, `[${zip}] Cleanup delete complete.`);
          line(controller, `[${zip}] ✅ Finished.`);
        }

        line(controller, "\n🎉 Done.");
        controller.close();
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        line(controller, `ERROR: ${msg}`);
        controller.close();
      }
    },
  });

  return new Response(stream, {
    status: 200,
    headers: {
      "content-type": "text/plain; charset=utf-8",
      "transfer-encoding": "chunked",
      "cache-control": "no-cache, no-transform",
    },
  });
}
