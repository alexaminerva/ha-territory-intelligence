const METABASE_URL = "https://metabase.houseaccount.com";
const METABASE_KEY = "mb_iw23KoeM5jenBBLSxwpIhMZBS3kTmyu5YNCSxN8Cvus=";
const DB_ID = 36;

async function sql(query) {
  const resp = await fetch(`${METABASE_URL}/api/dataset`, {
    method: "POST",
    headers: { "x-api-key": METABASE_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({ database: DB_ID, type: "native", native: { query } }),
  });
  const data = await resp.json();
  if (data.error) throw new Error(data.error);
  const cols = data.data.cols.map((c) => c.name);
  return data.data.rows.map((row) =>
    Object.fromEntries(cols.map((c, i) => [c, row[i]]))
  );
}

function sanitizeName(name) {
  return name.replace(/'/g, "''").replace(/[;\-\-]/g, "");
}

function validateDate(d) {
  return /^\d{4}-\d{2}-\d{2}$/.test(d) ? d : null;
}

export default async function handler(req, context) {
  if (req.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }

  try {
    const body = await req.json();
    const { mode, companyName, zips, dateFrom, dateTo, vertical } = body;

    // ── Schema probe ─────────────────────────────────────────────────────────
    if (mode === "schema") {
      const rows = await sql(`
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN ('bookings','providers','zips','categories','verticals','trades')
        ORDER BY table_name, ordinal_position
      `);
      return Response.json(rows);
    }

    // ── Booking status values ─────────────────────────────────────────────────
    if (mode === "statuses") {
      const rows = await sql(`SELECT DISTINCT status::text AS status FROM bookings WHERE status IS NOT NULL ORDER BY 1 LIMIT 50`);
      return Response.json(rows.map(r => r.status));
    }

    // ── Verticals list ────────────────────────────────────────────────────────
    if (mode === "verticals") {
      const rows = await sql(`
        SELECT DISTINCT v.name
        FROM verticals v
        WHERE v.name IS NOT NULL AND v.name != ''
        ORDER BY v.name
      `);
      return Response.json(rows.map((r) => r.name));
    }

    // ── All providers list ────────────────────────────────────────────────────
    if (mode === "allProviders") {
      const rows = await sql(`
        SELECT DISTINCT name FROM providers
        WHERE name IS NOT NULL AND name != ''
        ORDER BY name
        LIMIT 2000
      `);
      return Response.json(rows.map((r) => r.name));
    }

    // ── Provider search autocomplete ──────────────────────────────────────────
    if (mode === "search") {
      const term = sanitizeName((body.term || "").trim());
      if (term.length < 2) return Response.json([]);
      const rows = await sql(`
        SELECT DISTINCT name FROM providers
        WHERE LOWER(name) LIKE LOWER('%${term}%')
        ORDER BY name
        LIMIT 25
      `);
      return Response.json(rows.map((r) => r.name));
    }

    if (!companyName || companyName.trim().length < 2) {
      return Response.json({ error: "Company name is required (min 2 chars)" }, { status: 400 });
    }
    if (!validateDate(dateFrom) || !validateDate(dateTo)) {
      return Response.json({ error: "Invalid date format" }, { status: 400 });
    }

    const fn = sanitizeName(companyName.trim());
    const vn = vertical ? sanitizeName(vertical.trim()) : null;

    // Optional vertical filter — limits to providers who have this trade
    const verticalFilter = vn
      ? `AND b.provider_id IN (
           SELECT t.provider_id FROM trades t
           JOIN verticals v ON v.id = t.vertical_id
           WHERE LOWER(v.name) = LOWER('${vn}')
         )`
      : "";

    // ── Determine zip set ─────────────────────────────────────────────────────
    let zipSet = [];

    if (mode === "zips") {
      zipSet = (zips || []).map((z) => String(z).padStart(5, "0")).filter((z) => /^\d{5}$/.test(z));
      if (zipSet.length === 0) {
        return Response.json({ error: "No valid 5-digit zip codes found" }, { status: 400 });
      }
    } else {
      // Use all-time bookings for territory discovery so inactive-in-period
      // providers still contribute their zip coverage to the market query.
      const zipRows = await sql(`
        SELECT DISTINCT z.code AS zip
        FROM bookings b
        JOIN providers p ON p.id = b.provider_id
        JOIN zips z ON z.id = b.zip_id
        WHERE LOWER(p.name) LIKE LOWER('%${fn}%')
          ${verticalFilter}
        ORDER BY z.code
        LIMIT 1000
      `);
      zipSet = zipRows.map((r) => String(r.zip).padStart(5, "0"));

      if (zipSet.length === 0) {
        return Response.json({
          market: [], signed: [], zips: [],
          message: `No bookings found for providers matching "${companyName}"${vn ? ` in vertical "${vn}"` : ""}.`,
        });
      }
    }

    const zipSQL = zipSet.map((z) => `'${z}'`).join(",");

    // ── Market query — HA activity in these zips, excl. this company ─────────
    const marketRows = await sql(`
      SELECT z.code AS zip, z.city,
             COUNT(b.id) AS bookings,
             COALESCE(SUM((lower(b.estimate)+upper(b.estimate))/2.0), 0) AS revenue,
             COUNT(CASE WHEN b.status::text = 'fulfilled' THEN 1 END) AS fulfilled_bookings,
             COALESCE(SUM(CASE WHEN b.status::text = 'fulfilled' THEN (lower(b.estimate)+upper(b.estimate))/2.0 ELSE 0 END), 0) AS fulfilled_revenue
      FROM bookings b
      JOIN zips z ON z.id = b.zip_id
      WHERE z.code IN (${zipSQL})
        AND b.created_at BETWEEN '${dateFrom}' AND '${dateTo}'
        AND b.provider_id NOT IN (
          SELECT id FROM providers WHERE LOWER(name) LIKE LOWER('%${fn}%')
        )
        ${verticalFilter}
      GROUP BY z.code, z.city
      ORDER BY revenue DESC
    `);

    // ── Signed query — starts from providers table so zero-booking providers appear
    // LEFT JOIN to bookings means providers with no bookings still get a row (zip=null).
    // CASE WHEN scopes the aggregated metrics to the selected date range.
    const signedRows = await sql(`
      SELECT p.name AS provider,
             p.id AS provider_id,
             z.code AS zip,
             z.city,
             COUNT(CASE WHEN b.created_at BETWEEN '${dateFrom}' AND '${dateTo}' THEN b.id END) AS bookings,
             COALESCE(SUM(CASE WHEN b.created_at BETWEEN '${dateFrom}' AND '${dateTo}' THEN (lower(b.estimate)+upper(b.estimate))/2.0 ELSE 0 END), 0) AS revenue,
             COUNT(CASE WHEN b.created_at BETWEEN '${dateFrom}' AND '${dateTo}' AND b.status::text = 'fulfilled' THEN b.id END) AS fulfilled_bookings,
             COALESCE(SUM(CASE WHEN b.created_at BETWEEN '${dateFrom}' AND '${dateTo}' AND b.status::text = 'fulfilled' THEN (lower(b.estimate)+upper(b.estimate))/2.0 ELSE 0 END), 0) AS fulfilled_revenue
      FROM providers p
      LEFT JOIN bookings b ON b.provider_id = p.id
      LEFT JOIN zips z ON z.id = b.zip_id
      WHERE LOWER(p.name) LIKE LOWER('%${fn}%')
      GROUP BY p.name, p.id, z.code, z.city
      ORDER BY revenue DESC
    `);

    // Include signed provider zips in returned zip list so state tabs show them
    const signedZips = signedRows
      .filter((r) => r.zip)
      .map((r) => String(r.zip).padStart(5, "0"));
    const allZips = [...new Set([...zipSet, ...signedZips])];

    return Response.json({ market: marketRows, signed: signedRows, zips: allZips });
  } catch (err) {
    console.error(err);
    return Response.json({ error: err.message }, { status: 500 });
  }
}
