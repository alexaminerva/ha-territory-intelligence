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
    const { mode, companyName, zips, dateFrom, dateTo } = body;

    // ── Provider search autocomplete ─────────────────────────────────────────
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

    // ── Determine zip set ────────────────────────────────────────────────────
    let zipSet = [];

    if (mode === "zips") {
      // User provided the zips
      zipSet = (zips || []).map((z) => String(z).padStart(5, "0")).filter((z) => /^\d{5}$/.test(z));
      if (zipSet.length === 0) {
        return Response.json({ error: "No valid 5-digit zip codes found" }, { status: 400 });
      }
    } else {
      // mode === "provider" — discover zips from DB where this company has been active
      const zipRows = await sql(`
        SELECT DISTINCT z.code AS zip
        FROM bookings b
        JOIN providers p ON p.id = b.provider_id
        JOIN zips z ON z.id = b.zip_id
        WHERE LOWER(p.name) LIKE LOWER('%${fn}%')
          AND b.created_at BETWEEN '${dateFrom}' AND '${dateTo}'
        ORDER BY z.code
        LIMIT 1000
      `);
      zipSet = zipRows.map((r) => String(r.zip).padStart(5, "0"));

      if (zipSet.length === 0) {
        return Response.json({
          market: [], signed: [], zips: [],
          message: `No bookings found for providers matching "${companyName}" in this date range.`,
        });
      }
    }

    const zipSQL = zipSet.map((z) => `'${z}'`).join(",");

    // ── Market query ─────────────────────────────────────────────────────────
    // All HA activity in these zips, EXCLUDING this company's own providers
    const marketRows = await sql(`
      SELECT z.code AS zip, z.city,
             COUNT(b.id) AS bookings,
             COALESCE(SUM((lower(b.estimate)+upper(b.estimate))/2.0), 0) AS revenue
      FROM bookings b
      JOIN zips z ON z.id = b.zip_id
      WHERE z.code IN (${zipSQL})
        AND b.created_at BETWEEN '${dateFrom}' AND '${dateTo}'
        AND b.provider_id NOT IN (
          SELECT id FROM providers WHERE LOWER(name) LIKE LOWER('%${fn}%')
        )
      GROUP BY z.code, z.city
      ORDER BY revenue DESC
    `);

    // ── Signed query ─────────────────────────────────────────────────────────
    // Activity driven FOR this company's providers in these zips
    const signedRows = await sql(`
      SELECT p.name AS provider, z.code AS zip, z.city,
             COUNT(b.id) AS bookings,
             COALESCE(SUM((lower(b.estimate)+upper(b.estimate))/2.0), 0) AS revenue
      FROM bookings b
      JOIN providers p ON p.id = b.provider_id
      JOIN zips z ON z.id = b.zip_id
      WHERE z.code IN (${zipSQL})
        AND b.created_at BETWEEN '${dateFrom}' AND '${dateTo}'
        AND LOWER(p.name) LIKE LOWER('%${fn}%')
      GROUP BY p.name, z.code, z.city
      ORDER BY revenue DESC
    `);

    return Response.json({ market: marketRows, signed: signedRows, zips: zipSet });
  } catch (err) {
    console.error(err);
    return Response.json({ error: err.message }, { status: 500 });
  }
}
