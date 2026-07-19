/**
 * integration-worker — Cloudflare Worker
 *
 * Called via Service Binding from queue-worker (POST /deliver) — one call
 * per (lead, destination) pair for a CRM push, HubSpot/CMS/VMG, routed by
 * dest.type. No dispatching, no dedup decisions, no fetching from Seriti.
 * It receives fully-resolved calls (destination credentials already merged
 * with shared credentials upstream in cron-worker) and either successfully
 * delivers or returns an error for the caller to handle.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * NO LONGER A QUEUE CONSUMER — converted to a Service Binding target
 * ─────────────────────────────────────────────────────────────────────────
 * Originally consumed integration-queue. Converted because Cloudflare
 * Queues costs ~3 operations per message with a 10k/day budget on the
 * free plan — per-lead traffic through queues at every hand-off in this
 * pipeline blew well past that. Service Binding calls are billed as
 * ordinary Workers requests instead. See cron-worker's file header
 * "QUEUES OPERATIONS BUDGET" note for the full numbers and reasoning.
 *
 * RETRY IS NO LONGER AUTOMATIC: a queue gave failed messages automatic
 * retry with backoff. Now, if this Worker returns a non-2xx response,
 * queue-worker just logs it and moves on — the actual retry happens at
 * cron-worker's level, on its next dispatch tick (up to 30 min later),
 * since a failure here means cron-worker's lead-level dedup marker never
 * gets written for that lead.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * WHY THIS IS SEPARATE FROM digest-worker
 * ─────────────────────────────────────────────────────────────────────────
 * The old single-worker design needed max_concurrency: 1 on its lead
 * delivery queue SOLELY because the email digest destination accumulated
 * into a single shared KV key via read-modify-write, which isn't safe under
 * concurrent invocations. Now that email digest lives entirely in its own
 * Worker with its own accumulation logic, THIS Worker has no shared mutable
 * state at all — every call here is independent (create one contact,
 * submit one lead).
 *
 * ─────────────────────────────────────────────────────────────────────────
 * CALL CONTRACT — POST /deliver (from queue-worker)
 * ─────────────────────────────────────────────────────────────────────────
 * Body: { dealerKey, branchCode, intent, dest, lead, approvalChance, cacheKey }
 * — dest is one of { type: "hubspot", hubspotToken } |
 *                   { type: "cms", cmsToken, dealerRef, dealerFloorNew,
 *                     dealerFloorUsed, source, ... } |
 *                   { type: "vmg", vmgUsername, vmgPassword, dealerId, ... }
 * — cacheKey is written to LEADS_SYNC_CACHE with value "1" on success, so
 *   the upstream dedup check (in queue-worker) never re-sends this same
 *   lead-destination pair again. This Worker OWNS marking success — nothing
 *   upstream does it, since only this Worker actually knows the send
 *   succeeded. Returns 200 on success, non-2xx on failure (caller does not
 *   retry automatically — see note above).
 *
 * REQUIRED wrangler.toml:
 *   [[kv_namespaces]] binding = "LEADS_SYNC_CACHE"  (mark done on success)
 *   [[kv_namespaces]] binding = "VMG_AUTH_CACHE"    (VMG token cache)
 */

const DONE_MARKER_TTL = 604800; // 7 days — matches the dedup cache's existing TTL scheme.

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/deliver" && request.method === "POST") {
      let body;
      try {
        body = await request.json();
        await processIntegrationMessage(body, env);
        return new Response("OK", { status: 200 });
      } catch (err) {
        const { dealerKey, branchCode, dest, lead, cacheKey } = body || {};
        const label = branchCode ? `${dealerKey} [${branchCode}]` : dealerKey;
        console.error(`❌ [integration] Delivery failed for ${label} → ${dest?.type}: ${err.message}. Lead: ${lead?.firstName} ${lead?.lastName} (${lead?.mobileNumber}). Cache key: ${cacheKey}.`);
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        });
      }
    }

    return new Response("integration-worker", { status: 200 });
  },
};

async function processIntegrationMessage(msg, env) {
  const { dest, lead, intent, approvalChance, cacheKey } = msg;

  switch (dest.type) {
    case "hubspot":
      await sendToHubSpot(lead, intent, approvalChance, dest);
      break;
    case "cms":
      await sendToCMS(lead, intent, approvalChance, dest);
      break;
    case "vmg":
      await sendToVMG(lead, intent, approvalChance, dest, env);
      break;
    default:
      throw new Error(`Unknown destination type: ${dest.type} — this Worker only handles hubspot/cms/vmg. Email digest lives in digest-worker.`);
  }

  await env.LEADS_SYNC_CACHE.put(cacheKey, "1", { expirationTtl: DONE_MARKER_TTL });
}

// ─── Approval chance → CMS credit grading ──────────────────────────────────────
// ASSUMPTION: handles two possible shapes for approvalChance since the exact
// format Seriti returns isn't confirmed — (a) a qualitative label already
// ("High"/"Medium"/"Low") or (b) a numeric percentage/fraction.
function mapApprovalChanceToCreditGrading(approvalChance) {
  if (approvalChance === null || approvalChance === undefined || approvalChance === "null") {
    return "Unknown";
  }
  const raw = String(approvalChance).trim().toLowerCase();
  if (raw.includes("high")) return "Good";
  if (raw.includes("medium") || raw.includes("moderate")) return "Average";
  if (raw.includes("low")) return "Poor";
  const numeric = parseFloat(raw.replace(/[^0-9.\-]/g, ""));
  if (!Number.isNaN(numeric)) {
    const pct = numeric <= 1 ? numeric * 100 : numeric;
    if (pct >= 70) return "Good";
    if (pct >= 40) return "Average";
    return "Poor";
  }
  return "Unknown";
}

// ─── HubSpot ──────────────────────────────────────────────────────────────────
async function sendToHubSpot(lead, intent, approvalChance, dest) {
  const hubspotToken = dest.hubspotToken;

  const searchRes = await fetch("https://api.hubapi.com/crm/v3/objects/contacts/search", {
    method: "POST",
    headers: { Authorization: `Bearer ${hubspotToken}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      filterGroups: [{ filters: [{ propertyName: "mobilephone", operator: "EQ", value: lead.mobileNumber }] }],
      properties: ["id", "mobilephone"],
      limit: 1,
    }),
  });
  const searchData = await searchRes.json();
  if (searchData.total > 0) {
    console.log(`  ⏭️  [hubspot] Contact already exists (mobile: ${lead.mobileNumber}), skipping.`);
    return null;
  }

  let properties = {
    firstname: lead.firstName ?? null,
    lastname: lead.lastName ?? null,
    mobilephone: lead.mobileNumber ?? null,
    email: lead.emailAddress ?? null,
    seriti_id_number: lead.idNumber ?? null,
    estimated_finance: lead.estimatedAmount ?? null,
    kredo_predicted_approval: approvalChance ?? null,
    seriti_dealer_name: lead.dealerName ?? null,
    seriti_dealer_code: lead.dealerCode ?? null,
    seriti_lead_date: lead.date ?? null,
    lead_intent: intent === "highIntent" ? "High Intent" : "Low Intent",
  };

  let contact = await createHubSpotContact(hubspotToken, properties);

  // Self-healing: if the portal is missing one or more custom properties,
  // HubSpot returns a 400 naming exactly which ones don't exist. Strip only
  // those and retry once, rather than failing the whole contact.
  if (contact.status === 400 && contact.errorCode === "VALIDATION_ERROR") {
    const missingProps = (contact.errors || [])
      .filter(e => e.code === "PROPERTY_DOESNT_EXIST")
      .map(e => e.context?.propertyName?.[0])
      .filter(Boolean);

    if (missingProps.length > 0) {
      console.log(`  ⚠️  [hubspot] Portal is missing propert${missingProps.length > 1 ? "ies" : "y"}: ${missingProps.join(", ")} — retrying without ${missingProps.length > 1 ? "them" : "it"}.`);
      const trimmedProperties = { ...properties };
      for (const prop of missingProps) delete trimmedProperties[prop];
      contact = await createHubSpotContact(hubspotToken, trimmedProperties);
    }
  }

  if (contact.status && contact.status !== 200) {
    throw new Error(`HubSpot contact creation failed: ${contact.status} — ${JSON.stringify(contact.body)}`);
  }

  console.log(`  ✅ [hubspot] Contact created: ID ${contact.id}`);
  return contact;
}

async function createHubSpotContact(hubspotToken, properties) {
  const createRes = await fetch("https://api.hubapi.com/crm/v3/objects/contacts", {
    method: "POST",
    headers: { Authorization: `Bearer ${hubspotToken}`, "Content-Type": "application/json" },
    body: JSON.stringify({ properties }),
  });
  const body = await createRes.json().catch(() => ({}));
  if (!createRes.ok) {
    return { status: createRes.status, errorCode: body.category, errors: body.errors, body };
  }
  return body;
}

// ─── CMS LMS ──────────────────────────────────────────────────────────────────
function resolveDealerFloor(lead, dest) {
  const condition = String(lead.vehicleCondition || "").trim().toLowerCase();
  if (condition === "used" && dest.dealerFloorUsed) return dest.dealerFloorUsed;
  if (condition === "new" && dest.dealerFloorNew) return dest.dealerFloorNew;
  return dest.dealerFloor || dest.dealerFloorUsed || dest.dealerFloorNew || "";
}

async function sendToCMS(lead, intent, approvalChance, dest) {
  const endpoint = dest.endpoint || "https://leadsv3.cmscloud.co.za/api/lead/savelead";
  const condition = String(lead.vehicleCondition || "").trim().toLowerCase();

  const payload = {
    lead: {
      dealerRef: dest.dealerRef,
      dealerFloor: resolveDealerFloor(lead, dest),
      dealerSalesPerson: "",
      region: dest.region || lead.region || "",
      source: dest.source || "seriti-e-fficient",
      transactionID: "",
      extLeadRef: lead.idNumber || lead.mobileNumber || "",
      promotionalCode: "",
      utmParameters: "",
      countryCode: dest.countryCode || "ZAF",
      leadPostbackReference: "",
      contact: {
        title: lead.title || "",
        firstName: lead.firstName,
        surname: lead.lastName,
        email: lead.emailAddress || "",
        officePhone: "",
        cellPhone: lead.mobileNumber,
        driversLicense: "",
        incomeBracket: "",
        preferredContactMethod: "Cellphone",
        preferredContactTime: "",
        citizenship: "",
        idNo: lead.idNumber || "",
        birthDate: "",
        gender: "",
        ethnicity: "",
        homeLanguage: "",
        residentialAddressLine1: "",
        residentialAddressLine2: "",
        residentialAddressSuburb: "",
        residentialAddressCity: "",
        residentialAddressPostalCode: "",
        residentialAddressProvince: "",
        postalAddressLine1: "",
        postalAddressLine2: "",
        postalAddressSuburb: "",
        postalAddressCity: "",
        postalAddressCode: "",
        postalAddressProvince: "",
        marketingConsent: "",
        marketingConsentPhone: "",
        marketingConsentSMS: "",
        marketingConsentEmail: "",
        marketingConsentWhatsapp: "",
        creditGrading: mapApprovalChanceToCreditGrading(lead.approvalChance),
        companyName: "",
        companyType: "",
      },
      seeks: {
        used: condition === "new" ? "0" : condition === "used" ? "1" : (dest.defaultSeeksUsed ?? "1"),
        brand: lead.vehicleMake || "None",
        modelrange: "",
        model: lead.vehicleModel || "None",
        mmCode: "",
        modelCode: "",
        kms: "",
        year: "",
        colour: "",
        stockNr: "",
        price: lead.estimatedAmount || "",
        deposit: "",
        testDrive: "0",
        tradeIn: "",
        finance: "1",
        valuation: "",
        registration: "",
        special: "",
        specialBannerURL: "",
        serviceHistory: "",
        comments: `Lead intent: ${intent === "highIntent" ? "High Intent" : "Low Intent"}`,
        vin: "",
        regno: "",
        powertrain: "",
      },
    },
  };

  const res = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: dest.cmsToken },
    body: JSON.stringify(payload),
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.code !== "00") {
    throw new Error(`CMS lead injection failed: ${res.status} — ${data.message || JSON.stringify(data)}`);
  }

  console.log(`  ✅ [cms] Lead created: ref ${data.leadReference}`);
  return data;
}

// ─── VMG CRM ──────────────────────────────────────────────────────────────────
async function getVmgToken(dest, env) {
  const cacheKey = `vmg-token-${dest.vmgUsername}`;
  const cached = await env.VMG_AUTH_CACHE.get(cacheKey);
  if (cached) return cached;

  const res = await fetch("https://vmg.eu.auth0.com/oauth/ro", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      username: dest.vmgUsername,
      password: dest.vmgPassword,
      client_id: "HLATuHH7KaQNO3VFl0AH99JNJiN1nYTw",
      connection: "config-user-db",
      scope: "openid role",
      grant_type: "password",
    }),
  });
  if (!res.ok) throw new Error(`VMG auth failed: ${res.status}`);
  const data = await res.json();
  const token = data.id_token;
  if (!token) throw new Error(`VMG auth — no id_token: ${JSON.stringify(data)}`);

  await env.VMG_AUTH_CACHE.put(cacheKey, token, { expirationTtl: 3300 }); // id_token expires after 1hr; cached for 55min.
  return token;
}

async function sendToVMG(lead, intent, approvalChance, dest, env) {
  const idToken = await getVmgToken(dest, env);

  const payload = {
    dealer_id: dest.dealerId,
    lead_name: `${lead.firstName || ""} ${lead.lastName || ""}`.trim(),
    cellphone_no: lead.mobileNumber,
    email_address: lead.emailAddress || `${lead.mobileNumber}@no-email.findndrive.co.za`,
    lead_source: dest.leadSource || "seriti-e-fficient",
    region: lead.region || "",
    id_no: lead.idNumber || "",
    make: lead.vehicleMake || "None",
    model_desc: lead.vehicleModel || "None",
    message: `Lead intent: ${intent === "highIntent" ? "High Intent" : "Low Intent"}. Credit grading: ${mapApprovalChanceToCreditGrading(lead.approvalChance)}`,
  };

  const res = await fetch("https://api.vmgdms.com/leads/v1/new_dealer_lead", {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${idToken}` },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`VMG lead submission failed: ${res.status} — ${err}`);
  }

  console.log(`  ✅ [vmg] Lead submitted for dealer ${dest.dealerId}`);
  return res.json().catch(() => ({}));
}
