"use client";

import { useEffect, useMemo, useRef, useState, KeyboardEvent } from "react";
import { useRouter } from "next/navigation";
import { InfoIcon, PlayIcon, XIcon, DownloadIcon, Trash2Icon } from "lucide-react";
import { createClient } from "@/lib/supabase/client";

type CsvDownload = {
  filename: string;
  url: string; // object URL
  sizeBytes: number;
  zip: string;
};

export default function ProtectedPage() {
  const router = useRouter();
  const supabase = useMemo(() => createClient(), []);

  // ---- Auth / claims ----
  const [isAuthed, setIsAuthed] = useState<boolean | null>(null);
  const [claims, setClaims] = useState<Record<string, any> | null>(null);

  // ---- Pull Lists UI state ----
  const [token, setToken] = useState("");
  const [zips, setZips] = useState<string[]>([]);
  const [zipInput, setZipInput] = useState("");

  const [tags, setTags] = useState<string[]>([]);
  const [tagInput, setTagInput] = useState("");

  const [importToHL, setImportToHL] = useState(false); // (unused logic for now)

  // ---- Run/stream state ----
  const [isRunning, setIsRunning] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const [downloads, setDownloads] = useState<CsvDownload[]>([]);
  const logsRef = useRef<HTMLDivElement | null>(null);

  // ---- Effects ----
  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const { data: userData, error: userErr } = await supabase.auth.getUser();
        if (userErr || !userData?.user) {
          router.replace("/auth/login");
          return;
        }
        if (mounted) {
          setClaims({
            user_id: userData.user.id,
            email: userData.user.email,
          });
          setIsAuthed(true);
        }
      } catch {
        router.replace("/auth/login");
      }
    })();
    return () => {
      mounted = false;
    };
  }, [router, supabase]);

  // ---- Helpers ----
  const addZip = (raw: string) => {
    const v = raw.trim();
    if (!/^\d{5}$/.test(v)) return; // 5-digit only
    if (zips.includes(v)) return;
    setZips((prev) => [...prev, v]);
  };
  const removeZip = (v: string) => setZips((prev) => prev.filter((x) => x !== v));
  const onZipKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter" || e.key === ",") {
      e.preventDefault();
      const val = zipInput.replace(",", "").trim();
      if (val) addZip(val);
      setZipInput("");
    } else if (e.key === "Backspace" && zipInput.length === 0 && zips.length) {
      removeZip(zips[zips.length - 1]);
    }
  };

  const addTag = (raw: string) => {
    const v = raw.trim().toLowerCase();
    if (!v) return;
    if (tags.includes(v)) return;
    setTags((prev) => [...prev, v]);
  };
  const removeTag = (v: string) => setTags((prev) => prev.filter((x) => x !== v));
  const onTagKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter" || e.key === ",") {
      e.preventDefault();
      const val = tagInput.replace(",", "").trim();
      if (val) addTag(val);
      setTagInput("");
    } else if (e.key === "Backspace" && tagInput.length === 0 && tags.length) {
      removeTag(tags[tags.length - 1]);
    }
  };

  // Auto-scroll logs
  useEffect(() => {
    const el = logsRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [logs]);

  // Cleanup object URLs when downloads list changes or component unmounts
  useEffect(() => {
    return () => {
      downloads.forEach((d) => URL.revokeObjectURL(d.url));
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function onRun() {
    if (!token.trim()) {
      alert("Please enter your API token first.");
      return;
    }
    if (zips.length === 0) {
      alert("Please add at least one 5-digit ZIP.");
      return;
    }

    setIsRunning(true);
    setLogs([]);
    // Revoke old object URLs
    downloads.forEach((d) => URL.revokeObjectURL(d.url));
    setDownloads([]);

    try {
      const res = await fetch("/api/pull-lists/run", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          token: token.trim(),
          zips,
          // include tags/importToHL so we can wire up later if needed
          tags,
          importToHighLevel: importToHL,
        }),
      });

      if (!res.ok || !res.body) {
        const text = await res.text().catch(() => "");
        throw new Error(`Request failed: ${res.status} ${res.statusText}\n${text}`);
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        let idx: number;
        while ((idx = buffer.indexOf("\n")) >= 0) {
          const line = buffer.slice(0, idx);
          buffer = buffer.slice(idx + 1);
          handleStreamLine(line);
        }
      }

      // flush the last line if any
      if (buffer.trim().length) {
        handleStreamLine(buffer);
      }
    } catch (err: any) {
      setLogs((prev) => [...prev, `ERROR: ${String(err?.message || err)}`]);
    } finally {
      setIsRunning(false);
    }
  }

  function handleStreamLine(line: string) {
    if (!line) return;

    // Try JSON line first (NDJSON pattern for CSV payloads or structured events)
    try {
      const obj = JSON.parse(line);
      if (obj && obj.type === "csv" && typeof obj.zip === "string" && typeof obj.filename === "string" && typeof obj.dataBase64 === "string") {
        const bytes = atob(obj.dataBase64);
        const len = bytes.length;
        const arr = new Uint8Array(len);
        for (let i = 0; i < len; i++) arr[i] = bytes.charCodeAt(i);
        const blob = new Blob([arr], { type: "text/csv" });
        const url = URL.createObjectURL(blob);
        setDownloads((prev) => [
          ...prev,
          { filename: obj.filename, url, sizeBytes: blob.size, zip: obj.zip },
        ]);
        setLogs((prev) => [...prev, `CSV ready for ZIP ${obj.zip}: ${obj.filename} (${blob.size} bytes)`]);
        return;
      }

      if (obj && obj.type === "phase" && obj.message) {
        setLogs((prev) => [...prev, obj.message]);
        return;
      }
    } catch {
      // not JSON, fall through to plain log
    }

    // Plain text log line
    setLogs((prev) => [...prev, line]);
  }

  const isChecking = isAuthed === null;

  return (
    <div className="flex-1 w-full flex flex-col gap-8">
      {/* Top note */}
      <div className="w-full">
        <div className="bg-accent/30 text-sm p-3 px-5 rounded-md text-foreground/90 flex gap-3 items-center border border-accent/40">
          <InfoIcon size={16} strokeWidth={2} />
          <span>
            Pull Lists — live run shows here: pre-delete, build, scrape (CSV per ZIP).
          </span>
        </div>
      </div>

      {/* Pretty header */}
      <section className="relative overflow-hidden rounded-2xl border border-foreground/10 bg-gradient-to-tr from-muted/50 via-background to-muted/30 p-6">
        <div className="flex items-start justify-between gap-6">
          <div>
            <h1 className="text-2xl md:text-3xl font-semibold tracking-tight">
              Build a Pull List
            </h1>
            <p className="mt-2 text-sm opacity-70">
              Enter your token and ZIPs, then hit <span className="font-semibold">Run</span>. We’ll
              delete any existing leads first, then build and export per ZIP. (Tags and HighLevel are
              placeholders for now.)
            </p>
          </div>
          <div className="hidden md:flex items-center gap-2 text-xs opacity-70">
            <PlayIcon size={16} />
            streaming run
          </div>
        </div>
      </section>

      {/* Loading shim */}
      {isChecking ? (
        <section className="min-h-[30vh] grid place-items-center">
          <div className="animate-pulse text-sm opacity-70">Loading…</div>
        </section>
      ) : (
        <>
          {/* Form */}
          <section className="rounded-2xl border border-foreground/10 bg-background/60 backdrop-blur-sm p-6 shadow-sm">
            <form
              className="grid grid-cols-1 gap-6 md:grid-cols-2"
              onSubmit={(e) => {
                e.preventDefault();
                onRun();
              }}
            >
              {/* Token */}
              <div className="col-span-1 md:col-span-2">
                <label className="block text-sm font-medium mb-2">Token</label>
                <input
                  type="password"
                  value={token}
                  onChange={(e) => setToken(e.target.value)}
                  placeholder="Enter API token"
                  className="w-full rounded-xl border border-foreground/10 bg-background px-4 py-2.5 outline-none focus:ring-2 focus:ring-primary/40"
                />
                <p className="mt-1 text-xs opacity-60">Your secret token (hidden as you type).</p>
              </div>

              {/* ZIPs */}
              <div className="col-span-1">
                <label className="block text-sm font-medium mb-2">Zip code(s)</label>
                <div className="rounded-xl border border-foreground/10 bg-background px-3 py-2.5">
                  {zips.length > 0 && (
                    <div className="mb-2 flex flex-wrap gap-2">
                      {zips.map((v) => (
                        <span
                          key={v}
                          className="inline-flex items-center gap-2 rounded-full border border-foreground/10 bg-muted/50 px-3 py-1 text-xs"
                          title={v}
                        >
                          <span className="font-semibold opacity-70">ZIP:</span>
                          <span className="font-mono">{v}</span>
                          <button
                            type="button"
                            onClick={() => removeZip(v)}
                            className="hover:opacity-70"
                            aria-label={`Remove ${v}`}
                          >
                            <XIcon size={14} />
                          </button>
                        </span>
                      ))}
                    </div>
                  )}
                  <input
                    value={zipInput}
                    onChange={(e) => setZipInput(e.target.value)}
                    onKeyDown={onZipKeyDown}
                    placeholder="Type a ZIP and press Enter"
                    className="w-full bg-transparent outline-none text-sm placeholder:opacity-60"
                  />
                </div>
                <p className="mt-1 text-xs opacity-60">5-digit US ZIP codes only.</p>
              </div>

              {/* Tags (placeholder) */}
              <div className="col-span-1">
                <label className="block text-sm font-medium mb-2">Tags</label>
                <div className="rounded-xl border border-foreground/10 bg-background px-3 py-2.5">
                  {tags.length > 0 && (
                    <div className="mb-2 flex flex-wrap gap-2">
                      {tags.map((v) => (
                        <span
                          key={v}
                          className="inline-flex items-center gap-2 rounded-full border border-foreground/10 bg-muted/50 px-3 py-1 text-xs"
                          title={v}
                        >
                          <span className="font-semibold opacity-70">TAG:</span>
                          <span className="font-mono">{v}</span>
                          <button
                            type="button"
                            onClick={() => removeTag(v)}
                            className="hover:opacity-70"
                            aria-label={`Remove ${v}`}
                          >
                            <XIcon size={14} />
                          </button>
                        </span>
                      ))}
                    </div>
                  )}
                  <input
                    value={tagInput}
                    onChange={(e) => setTagInput(e.target.value)}
                    onKeyDown={onTagKeyDown}
                    placeholder="Type a tag and press Enter"
                    className="w-full bg-transparent outline-none text-sm placeholder:opacity-60"
                  />
                </div>
                <p className="mt-1 text-xs opacity-60">Any short keywords. Duplicates ignored.</p>
              </div>

              {/* Import to HighLevel (placeholder) */}
              <div className="col-span-1 md:col-span-2">
                <label className="inline-flex items-center gap-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={importToHL}
                    onChange={(e) => setImportToHL(e.target.checked)}
                    className="h-4 w-4 rounded border-foreground/20"
                  />
                  <span className="text-sm">Import to HighLevel</span>
                </label>
              </div>

              {/* Run button */}
              <div className="col-span-1 md:col-span-2 flex items-center gap-3">
                <button
                  type="submit"
                  disabled={isRunning}
                  className="inline-flex items-center gap-2 rounded-xl border border-foreground/10 bg-primary/90 px-5 py-2.5 text-sm font-medium text-primary-foreground shadow hover:brightness-110 active:brightness-95 transition disabled:opacity-50"
                >
                  <PlayIcon size={16} />
                  {isRunning ? "running…" : "run"}
                </button>

                {downloads.length > 0 && (
                  <div className="text-xs opacity-70">
                    {downloads.length} CSV {downloads.length === 1 ? "file" : "files"} ready
                  </div>
                )}
              </div>
            </form>
          </section>

          {/* Downloads list */}
          {downloads.length > 0 && (
            <section className="rounded-2xl border border-foreground/10 bg-background/60 backdrop-blur-sm p-4 shadow-sm">
              <h3 className="text-sm font-semibold mb-3 flex items-center gap-2">
                <DownloadIcon size={16} /> Downloads
              </h3>
              <ul className="space-y-2 text-sm">
                {downloads.map((d, i) => (
                  <li key={`${d.filename}-${i}`} className="flex items-center justify-between gap-3">
                    <div className="flex flex-col">
                      <span className="font-mono">{d.filename}</span>
                      <span className="opacity-60 text-xs">
                        ZIP {d.zip} — {d.sizeBytes.toLocaleString()} bytes
                      </span>
                    </div>
                    <div className="flex items-center gap-2">
                      <a
                        href={d.url}
                        download={d.filename}
                        className="px-3 py-1 rounded-lg border border-foreground/10 text-xs hover:bg-muted/40"
                      >
                        Download
                      </a>
                      <button
                        className="p-1 rounded hover:bg-muted/40"
                        onClick={() => {
                          URL.revokeObjectURL(d.url);
                          setDownloads((prev) => prev.filter((x) => x !== d));
                        }}
                        title="Remove"
                        aria-label="Remove"
                      >
                        <Trash2Icon size={14} />
                      </button>
                    </div>
                  </li>
                ))}
              </ul>
            </section>
          )}

          {/* Logs */}
          <section className="rounded-2xl border border-foreground/10 bg-background/60 backdrop-blur-sm p-4 shadow-sm">
            <h3 className="text-sm font-semibold mb-3">Run log</h3>
            <div
              ref={logsRef}
              className="h-64 overflow-auto rounded-lg border border-foreground/10 bg-muted/30 p-3 font-mono text-xs leading-5"
            >
              {logs.length === 0 ? (
                <div className="opacity-50">Logs will appear here…</div>
              ) : (
                logs.map((l, i) => <div key={i}>{l}</div>)
              )}
            </div>
          </section>

          {/* Debug of claims */}
          <section className="flex flex-col gap-2 items-start">
            <h2 className="font-bold text-lg">Your user details</h2>
            <pre className="text-xs font-mono p-3 rounded border max-h-32 overflow-auto w-full bg-muted/30">
{JSON.stringify(claims ?? {}, null, 2)}
            </pre>
          </section>
        </>
      )}
    </div>
  );
}
