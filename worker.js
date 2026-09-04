/**
 * integration-worker — Cloudflare Worker
 *
 * Called via Service Binding from queue-worker (POST /deliver) — one call
 * per (lead, destination) pair for a CRM push, HubSpot/CMS/VMG/DealerOS,
 * routed by dest.type. No dispatching, no dedup decisions, no fetching
 * from Seriti. It receives fully-resolved calls (destination credentials
 * already merged with shared credentials upstream in cron-worker) and
 * either successfully delivers or returns an error for the caller to handle.
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
 *                   { type: "vmg", vmgUsername, vmgPassword, dealerId, ... } |
 *                   { type: "dealeros", dealerosToken, dealershipId,
 *                     leadSource, enquiryMethod, ... }
 * — intent is normally "highIntent" or "lowIntent" (from the Seriti lead
 *   feed). It can also be "policySubmitted" — sent by the D1-driven
 *   policy-sync path for a completed application that never had a Seriti
 *   lead at all (Seriti drops a lead from its feed entirely once it
 *   converts to a policy). See buildIntentSummaryLine() below: this value
 *   deliberately skips intent/credit-grading messaging everywhere and just
 *   states "Application submitted: {policyNumber}" instead — these records
 *   were never scored highIntent/lowIntent and shouldn't be forced into
 *   either bucket.
 * — cacheKey is written to LEADS_SYNC_CACHE with value "1" on success, so
 *   the upstream dedup check (in queue-worker) never re-sends this same
 *   lead-destination pair again. This Worker OWNS marking success — nothing
 *   upstream does it, since only this Worker actually knows the send
 *   succeeded. Returns 200 on success, non-2xx on failure (caller does not
 *   retry automatically — see note above).
 *
 * — Response body on success: { ok: true, externalLeadId }, where
 *   externalLeadId is the CRM-assigned ID for the created contact/lead
 *   (HubSpot contact id, CMS leadReference, DealerOS lead_id, VMG's
 *   equivalent — null if the destination didn't return one, or for "email"
 *   which digest-worker handles, not this Worker).
 * — externalLeadId is also stored in LEADS_SYNC_CACHE under
 *   `${cacheKey}-external-id` (7-day TTL, same as the done marker) so it
 *   can be looked up later without digging through logs.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * CALL CONTRACT — POST /update-policy-number (from queue-worker, added 2026-08-29)
 * ─────────────────────────────────────────────────────────────────────────
 * Body: { dest, externalLeadId, policyNumber }
 * Attaches a policy number to a CRM record that was already created by a
 * prior /deliver call — dest and externalLeadId both come from that
 * earlier call's response (queue-worker looks externalLeadId back up from
 * LEADS_SYNC_CACHE, since it's not held anywhere else). This Worker does
 * NOT touch LEADS_SYNC_CACHE at all for this route — no cacheKey is even
 * passed in, since there's no new done/queued state to track.
 *
 * Only HubSpot is implemented for real right now — see
 * updatePolicyNumber() below. CMS/VMG/DealerOS return
 * { skipped: true, reason } rather than attempting anything, since none of
 * their update/PATCH endpoints have been confirmed against real docs (the
 * same category of gap already flagged for VMG's externalLeadId field
 * above). DealerOS in particular is confirmed to have NO update/note API
 * at all — its policy number only ever gets in via buildDealerOSMessage()
 * at lead CREATION time (see that function's comments), never via this
 * route. Wire the others up once their actual update APIs are confirmed —
 * don't guess at an endpoint shape the way this file's comments elsewhere
 * warn against for other unconfirmed integrations.
 *
 * Returns 200 with { ok: true, updated: true } (HubSpot) or
 * { ok: true, skipped: true, reason } (unimplemented destinations) on
 * success, non-2xx on genuine failure (e.g. HubSpot's PATCH itself erroring).
 *
 * ─────────────────────────────────────────────────────────────────────────
 * DEALEROS NOTES
 * ─────────────────────────────────────────────────────────────────────────
 * /api/lead/create requires vehicle_specifications.{year,make,model,new_used}
 * ALL FOUR together, OR lead.stock_number — we never have stock_number, so
 * vehicle_specifications is only sent when we have make+model. year is
 * resolved via resolveVehicleYear(): uses lead.vehicleYear if Seriti ever
 * supplies it, otherwise defaults to the current year for NEW and
 * (current year - 2) for USED, since Seriti/CMS leads carry condition but
 * not model year. dest.leadSource/dest.enquiryMethod must be valid DealerOS
 * enum values (see their docs) — invalid values 422 rather than failing
 * silently, and that will surface as a normal thrown error/log line below.
 * On success, DealerOS returns { status, success, data: { lead_id } } —
 * lead_id is pulled out as externalLeadId.
 *
 * DEALEROS FAILURE DIAGNOSTICS (added 2026-08-21): repeated 404s with an
 * empty {} body from this Worker, even using credentials confirmed working
 * via direct Postman calls — raised the possibility that DealerOS's
 * Cloudflare-fronted edge/WAF is treating server-to-server traffic from
 * this Worker differently than a browser/Postman client (blocking/
 * rerouting before the request reaches their app, returning an
 * under-informative 404 instead of 403/503). On any non-ok response, this
 * Worker now logs the full response header set (including cf-ray,
 * cf-mitigated, server) and the raw response body text, to distinguish an
 * edge-level block from an application-level 404. See
 * logDealerOSFailureDiagnostics() below.
 *
 * DEALEROS ENDPOINT — .co.za is the CORRECT/intended production domain,
 * NOT .click (2026-09-04 correction). Both domains genuinely exist and
 * both document the same /api/lead/create route, which is what caused
 * repeated confusion during the 2026-08-21/22 debugging session — the
 * default was flipped between the two more than once while chasing a
 * separate DealerOS 404/503 issue that turned out to be unrelated to
 * which domain was used (missing Accept header, then a genuine multi-day
 * DealerOS outage). If this ever needs revisiting, confirm directly with
 * Richard/DealerOS which domain the current token is actually issued
 * against — don't just flip it back to .click on a hunch, since that's
 * exactly what happened last time.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * VMG NOTE
 * ─────────────────────────────────────────────────────────────────────────
 * externalLeadId for VMG is a best-effort guess (data.lead_id ?? data.id)
 * since their success response shape wasn't confirmed against real docs
 * at the time this was written — confirm against a live response and
 * adjust the field name in sendToVMG() below if it's null in practice.
 *
 * REQUIRED wrangler.toml:
 *   [[kv_namespaces]] binding = "LEADS_SYNC_CACHE"  (mark done on success)
 *   [[kv_namespaces]] binding = "VMG_AUTH_CACHE"    (VMG token cache)
 */

const DONE_MARKER_TTL = 604800; // 7 days — matches the dedup cache's existing TTL scheme.

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/test-dealeros-minimal-lead" && request.method === "POST") {
      // Manual diagnostic trigger only — sends the smallest possible valid
      // lead payload (no vehicle_specifications, no external_reference, no
      // ID number) to isolate whether ANY payload shape succeeds against
      // /api/lead/create from this Worker, or whether the route itself
      // 404s regardless of payload content.
      try {
        const { dealerosToken, dealershipId } = await request.json();
        const endpoint = "https://api.dealeros.co.za/api/lead/create";
        const payload = {
          customer: { first_name: "FindDrive", last_name: "Test", cell_number: "+27720000000" },
          lead: {
            source: "DEALER_WEBSITE_ORGANIC",
            enquiry_method: "WEBSITE",
            message: "FindDrive API connectivity test",
            dealership_id: dealershipId || "2",
          },
        };
        const res = await fetch(endpoint, {
          method: "POST",
          redirect: "manual",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json",
            Authorization: `Bearer ${dealerosToken}`,
          },
          body: JSON.stringify(payload),
        });
        const body = await res.text();
        const headers = {};
        for (const [key, value] of res.headers.entries()) headers[key] = value;
        console.log(`🔬 [dealeros-test] MINIMAL-LEAD status=${res.status} ${res.statusText}`);
        console.log(`🔬 [dealeros-test] MINIMAL-LEAD location=${headers["location"] || "(none)"}`);
        console.log(`🔬 [dealeros-test] MINIMAL-LEAD body=${body}`);
        return new Response(JSON.stringify({ status: res.status, location: headers["location"] || null, headers, body }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        });
      }
    }

    if (url.pathname === "/test-dealeros-auth" && request.method === "POST") {
      // Manual diagnostic trigger only — never called by the normal lead
      // delivery flow. POST body: { dealerosToken }. Calls a known
      // authenticated GET endpoint (not lead creation) to isolate whether
      // a 404 on /api/lead/create is an auth/environment problem or a
      // routing problem specific to that one endpoint.
      try {
        const { dealerosToken } = await request.json();
        const result = await testDealerOSAuth(dealerosToken);
        return new Response(JSON.stringify(result), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      } catch (err) {
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        });
      }
    }

    if (url.pathname === "/deliver" && request.method === "POST") {
      let body;
      try {
        body = await request.json();
        const externalLeadId = await processIntegrationMessage(body, env);
        return new Response(JSON.stringify({ ok: true, externalLeadId }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
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

    if (url.pathname === "/update-policy-number" && request.method === "POST") {
      let body;
      try {
        body = await request.json();
        const { dest, externalLeadId, policyNumber } = body;
        const result = await updatePolicyNumber(dest, externalLeadId, policyNumber);
        return new Response(JSON.stringify({ ok: true, ...result }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      } catch (err) {
        const { dest, externalLeadId } = body || {};
        console.error(`❌ [integration] Policy number update failed for ${dest?.type} (${externalLeadId}): ${err.message}`);
        return new Response(JSON.stringify({ error: err.message }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        });
      }
    }

    return new Response("integration-worker", { status: 200 });
  },
};

// Routes to the right destination sender, marks the lead-destination pair
// done in the dedup cache, and — if the destination returned an external
// CRM lead ID — stores that too and returns it to the caller.
async function processIntegrationMessage(msg, env) {
  const { dest, lead, intent, approvalChance, cacheKey } = msg;
  let result;

  switch (dest.type) {
    case "hubspot":
      result = await sendToHubSpot(lead, intent, approvalChance, dest);
      break;
    case "cms":
      result = await sendToCMS(lead, intent, approvalChance, dest);
      break;
    case "vmg":
      result = await sendToVMG(lead, intent, approvalChance, dest, env);
      break;
    case "dealeros":
      result = await sendToDealerOS(lead, intent, approvalChance, dest);
      break;
    default:
      throw new Error(`Unknown destination type: ${dest.type} — this Worker only handles hubspot/cms/vmg/dealeros. Email digest lives in digest-worker.`);
  }

  await env.LEADS_SYNC_CACHE.put(cacheKey, "1", { expirationTtl: DONE_MARKER_TTL });

  const externalLeadId = result?.externalLeadId ?? null;
  if (externalLeadId) {
    await env.LEADS_SYNC_CACHE.put(`${cacheKey}-external-id`, String(externalLeadId), { expirationTtl: DONE_MARKER_TTL });
  }

  return externalLeadId;
}

// Routes a policy-number-only update to the right destination handler.
// See file header "CALL CONTRACT — POST /update-policy-number".
async function updatePolicyNumber(dest, externalLeadId, policyNumber) {
  if (!externalLeadId) throw new Error("updatePolicyNumber called without an externalLeadId");
  if (!policyNumber) throw new Error("updatePolicyNumber called without a policyNumber");

  switch (dest.type) {
    case "hubspot":
      return await updateHubSpotPolicyNumber(dest.hubspotToken, externalLeadId, policyNumber);
    case "cms":
    case "vmg":
    case "dealeros":
      // TODO: confirm each of these against real API docs before wiring
      // up — see file header for why this is deliberately left unimplemented
      // rather than guessed at.
      console.warn(`  ⚠️  [${dest.type}] policy number update not implemented — externalLeadId=${externalLeadId}, policyNumber=${policyNumber}`);
      return { skipped: true, reason: `${dest.type} policy number update endpoint not yet confirmed/implemented` };
    default:
      throw new Error(`Unknown destination type for policy number update: ${dest.type}`);
  }
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

// ─── Intent/policy summary line ────────────────────────────────────────────────
// Shared across CMS/VMG/HubSpot's free-text fields. A record pushed by the
// D1-driven policy-sync path (see cron-worker/policy-sync-worker) carries
// intent === "policySubmitted" — these never had a Seriti lead at all, so
// they were never scored highIntent/lowIntent and should NOT be forced into
// either bucket. For those, this returns just "Application submitted:
// {policyNumber}" with no intent/credit-grading noise. Everything else
// keeps the normal "Lead intent: X. Credit grading: Y" line — unchanged
// from before this policySubmitted case existed.
function buildIntentSummaryLine(lead, intent) {
  if (intent === "policySubmitted") {
    return `Application submitted: ${lead.policyNumber}`;
  }
  return `Lead intent: ${intent === "highIntent" ? "High Intent" : "Low Intent"}. Credit grading: ${mapApprovalChanceToCreditGrading(lead.approvalChance)}`;
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
    const existingId = searchData.results?.[0]?.id ?? null;
    console.log(`  ⏭️  [hubspot] Contact already exists (mobile: ${lead.mobileNumber}), skipping. Existing ID: ${existingId}`);
    return { externalLeadId: existingId };
  }

  let properties = {
    firstname: lead.firstName ?? null,
    lastname: lead.lastName ?? null,
    mobilephone: lead.mobileNumber ?? null,
    email: lead.emailAddress ?? null,
    seriti_id_number: lead.idNumber ?? null,
    estimated_finance: lead.estimatedAmount ?? null,
    kredo_predicted_approval: intent === "policySubmitted" ? null : (approvalChance ?? null),
    seriti_dealer_name: lead.dealerName ?? null,
    seriti_dealer_code: lead.dealerCode ?? null,
    seriti_lead_date: lead.date ?? null,
    lead_intent: intent === "policySubmitted" ? "Application Submitted" : (intent === "highIntent" ? "High Intent" : "Low Intent"),
    edith_policy_number: lead.policyNumber ?? null,
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
  return { ...contact, externalLeadId: contact.id ?? null };
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

// Attaches a policy number to an EXISTING HubSpot contact via PATCH.
// Requires a custom `edith_policy_number` property to exist on the portal
// (same property createHubSpotContact() writes to on initial creation, if
// it happens to be known at that point) — this call does NOT self-heal a
// missing property the way contact creation does, since dropping the one
// field being updated would leave nothing to send. If this 400s with
// PROPERTY_DOESNT_EXIST, create that custom property on the HubSpot portal
// first.
async function updateHubSpotPolicyNumber(hubspotToken, contactId, policyNumber) {
  const res = await fetch(`https://api.hubapi.com/crm/v3/objects/contacts/${contactId}`, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${hubspotToken}`, "Content-Type": "application/json" },
    body: JSON.stringify({ properties: { edith_policy_number: policyNumber } }),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(`HubSpot policy number update failed: ${res.status} — ${JSON.stringify(body)}`);
  }
  console.log(`  ✅ [hubspot] Policy number ${policyNumber} attached to contact ${contactId}`);
  return { updated: true };
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
        comments: `${buildIntentSummaryLine(lead, intent)}`,
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
  return { ...data, externalLeadId: data.leadReference ?? null };
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
    message: intent === "policySubmitted"
      ? buildIntentSummaryLine(lead, intent)
      : `${buildIntentSummaryLine(lead, intent)}${lead.policyNumber ? `. Policy number: ${lead.policyNumber}` : ""}`,
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

  const data = await res.json().catch(() => ({}));
  // TODO: confirm actual VMG response field name against a live success
  // response — this is a best-effort guess (lead_id, falling back to id).
  const externalLeadId = data?.lead_id ?? data?.id ?? null;
  console.log(`  ✅ [vmg] Lead submitted for dealer ${dest.dealerId} — external ID: ${externalLeadId}`);
  return { ...data, externalLeadId };
}

// ─── DealerOS ─────────────────────────────────────────────────────────────────
// year defaulting: Seriti/CMS leads carry vehicleCondition but never a
// model year. If lead.vehicleYear is ever populated upstream (from stock
// data in any of our DBs), it always wins. Otherwise these are HARDCODED
// fallback values, not a relative "current year" formula — 2024 for USED,
// 2026 for NEW, reflecting actual current stock years as of 2026-09-04.
// These do NOT auto-update — if it's been a while since that date, check
// with the team whether these are still the right numbers before assuming
// they are. Everything else — explicit NEW, or a skipped vehicle-selection
// page where vehicleCondition is empty/undefined — defaults to the NEW
// value, same reasoning as before: a skipped condition is NOT the same
// signal as a confirmed used vehicle, so it shouldn't silently inherit the
// used-vehicle default.
function resolveVehicleYear(lead) {
  if (lead.vehicleYear) return String(lead.vehicleYear);
  const isUsed = String(lead.vehicleCondition || "").trim().toUpperCase() === "USED";
  return isUsed ? "2024" : "2026";
}

// DealerOS has no dedicated finance/approval-amount field anywhere in its
// schema (customer/lead/vehicle_specifications) — same situation as lead
// intent and credit grading, so it's folded into lead.message (max 1000
// chars) rather than dropped. Only appended when present, since not every
// lead in the pipeline has an estimatedAmount from Seriti.
//
// POLICY NUMBER: DealerOS only has this message field available at lead
// CREATION — there's no confirmed update/note API to touch it afterward
// (see /update-policy-number's dealeros case, which stays a no-op for
// exactly this reason). So this only ever matters for the rare case where
// a policy number is already known before the lead is first synced (see
// cron-worker's "POLICY NUMBER BACKFILL" file header) — when that
// happens, the message is replaced entirely rather than appended to,
// since "Lead intent / Credit grading" is no longer the useful summary
// once the application itself is already done.
// DealerOS has no dedicated finance field anywhere in its schema
// (customer/lead/vehicle_specifications) — same situation as lead intent
// and credit grading, so net income is folded into lead.message (max 1000
// chars) rather than dropped. Estimated approval amount was previously
// included in the low/high-intent message too but was deliberately
// removed (2026-09-04) — only net income appears there now. Net income is
// still appended across all three message variants below (low intent,
// high intent, and the completed-application variant), whenever
// lead.netIncome is present.
//
// POLICY NUMBER: DealerOS only has this message field available at lead
// CREATION — there's no confirmed update/note API to touch it afterward
// (see /update-policy-number's dealeros case, which stays a no-op for
// exactly this reason). So this only ever matters for the rare case where
// a policy number is already known before the lead is first synced (see
// cron-worker's "POLICY NUMBER BACKFILL" file header) — when that
// happens, the message is replaced entirely rather than appended to,
// since "Lead intent / Credit grading" is no longer the useful summary
// once the application itself is already done. Net income still gets
// appended to that replaced message too, when available.
function buildDealerOSMessage(lead, intent) {
  if (lead.policyNumber) {
    const parts = [
      `Application submitted: ${lead.policyNumber}`,
      `Credit grading: ${mapApprovalChanceToCreditGrading(lead.approvalChance)}`,
    ];
    if (lead.netIncome) {
      parts.push(`Net income: R${lead.netIncome}`);
    }
    return parts.join(". ");
  }

  const parts = [
    `Lead intent: ${intent === "highIntent" ? "High Intent" : "Low Intent"}`,
    `Credit grading: ${mapApprovalChanceToCreditGrading(lead.approvalChance)}`,
  ];
  if (lead.netIncome) {
    parts.push(`Net income: R${lead.netIncome}`);
  }
  return parts.join(". ");
}

// Logs the full failure context for a non-ok DealerOS response: every
// response header (headers are the whole point here — cf-ray identifies
// the exact edge request for DealerOS/Cloudflare support to look up,
// cf-mitigated or server values can reveal an edge/WAF-level block rather
// than the request reaching DealerOS's actual application), the raw body
// text (in case it's not valid JSON, which a WAF block page often isn't),
// and the exact request we sent (headers + payload) for side-by-side
// comparison against a known-working Postman call.
async function logDealerOSFailureDiagnostics(res, rawBodyText, endpoint, requestHeaders, payload) {
  const responseHeaders = {};
  for (const [key, value] of res.headers.entries()) {
    responseHeaders[key] = value;
  }

  console.error(`  🔍 [dealeros] DIAGNOSTICS — non-ok response detail:`);
  console.error(`  🔍 [dealeros] Request: POST ${endpoint}`);
  console.error(`  🔍 [dealeros] Request headers: ${JSON.stringify(requestHeaders)}`); // caller passes the REDACTED copy — see sendToDealerOS.
  console.error(`  🔍 [dealeros] Request payload: ${JSON.stringify(payload)}`);
  console.error(`  🔍 [dealeros] Response status: ${res.status} ${res.statusText}`);
  console.error(`  🔍 [dealeros] Response headers: ${JSON.stringify(responseHeaders)}`);
  console.error(`  🔍 [dealeros] Response cf-ray: ${responseHeaders["cf-ray"] ?? "(none)"}`);
  console.error(`  🔍 [dealeros] Response cf-mitigated: ${responseHeaders["cf-mitigated"] ?? "(none)"}`);
  console.error(`  🔍 [dealeros] Response server: ${responseHeaders["server"] ?? "(none)"}`);
  console.error(`  🔍 [dealeros] Raw response body: ${rawBodyText}`);
}

// ─── DealerOS diagnostic: auth/environment isolation tests ────────────────────
// Both call known-authenticated, non-mutating DealerOS endpoints (neither
// creates a lead) with the same Bearer token used for lead creation, to
// distinguish "token/environment isn't valid here at all" (401/302/404 on
// THESE endpoints too) from "token+environment are fine, but
// /api/lead/create specifically doesn't route" (200 here, 404 there).
// redirect: "manual" is deliberate — DealerOS's docs show authenticated
// failures can 302 to /login; letting fetch() auto-follow that would hide
// the redirect and just show us whatever /login itself returns. Manual
// trigger only — see /test-dealeros-auth in the fetch handler above. Token
// is redacted in all logging (only first 8 chars + "...REDACTED").
async function runDealerOSDiagnosticCall(label, endpoint, dealerosToken) {
  const res = await fetch(endpoint, {
    method: "GET",
    redirect: "manual",
    headers: {
      Authorization: `Bearer ${dealerosToken}`,
      Accept: "application/json",
    },
  });

  const body = await res.text();
  const headers = {};
  for (const [key, value] of res.headers.entries()) {
    headers[key] = value;
  }

  console.log(`🔬 [dealeros-test] ${label} status=${res.status} ${res.statusText}`);
  console.log(`🔬 [dealeros-test] ${label} location=${headers["location"] || "(none)"}`);
  console.log(`🔬 [dealeros-test] ${label} headers=${JSON.stringify(headers)}`);
  console.log(`🔬 [dealeros-test] ${label} body=${body}`);

  return { status: res.status, location: headers["location"] || null, headers, body };
}

async function testDealerOSAuth(dealerosToken) {
  const redactedToken = dealerosToken ? `${dealerosToken.slice(0, 8)}...REDACTED` : "(missing)";
  console.log(`🔬 [dealeros-test] Token: ${redactedToken}`);

  const profile = await runDealerOSDiagnosticCall(
    "PROFILE",
    "https://api.dealeros.co.za/api/profile/linked-dealerships",
    dealerosToken
  );
  const listings = await runDealerOSDiagnosticCall(
    "LISTINGS",
    "https://api.dealeros.co.za/api/listings/list",
    dealerosToken
  );

  return { profile, listings };
}

async function sendToDealerOS(lead, intent, approvalChance, dest) {
  // See file header "DEALEROS ENDPOINT" — .co.za is the correct
  // production domain, confirmed 2026-09-04.
  const endpoint = dest.endpoint || "https://api.dealeros.co.za/api/lead/create";
  const uniqueId = lead.idNumber || lead.mobileNumber || "unknown";

  const payload = {
    customer: {
      first_name: lead.firstName,
      last_name: lead.lastName || undefined,
      email: lead.emailAddress || undefined,
      cell_number: lead.mobileNumber || undefined,
      national_id_number: lead.idNumber || undefined,
    },
    lead: {
      source: dest.leadSource || "DEALER_WEBSITE_ORGANIC",
      enquiry_method: dest.enquiryMethod || "WEBSITE",
      message: buildDealerOSMessage(lead, intent),
      dealership_id: dest.dealershipId,
      external_reference: `${uniqueId}-${lead.date}`, // lets DealerOS dedupe on retry, if they honour it
    },
  };

  // vehicle_specifications requires year+make+model+new_used TOGETHER, or
  // lead.stock_number (which we never have). Only attach it when we have
  // make+model to work with — year is always resolvable via the default.
  if (lead.vehicleMake && lead.vehicleModel) {
    payload.vehicle_specifications = {
      year: resolveVehicleYear(lead),
      make: lead.vehicleMake,
      model: lead.vehicleModel,
      new_used: String(lead.vehicleCondition || "").trim().toUpperCase() === "NEW" ? "NEW" : "USED",
    };
  }

  const requestHeaders = {
    "Content-Type": "application/json",
    Accept: "application/json",
    Authorization: `Bearer ${dest.dealerosToken}`,
  };

  // Redacted copy used ONLY for logging — never log requestHeaders itself,
  // since it carries the live bearer token.
  const redactedToken = dest.dealerosToken ? `${dest.dealerosToken.slice(0, 8)}...REDACTED` : "(missing)";
  const requestHeadersForLog = { ...requestHeaders, Authorization: `Bearer ${redactedToken}` };

  const res = await fetch(endpoint, {
    method: "POST",
    headers: requestHeaders,
    body: JSON.stringify(payload),
  });

  // Read the body ONCE as text first — res.json() consumes the stream, and
  // if the body isn't valid JSON (e.g. a WAF/edge block page, which is
  // often HTML or plain text) a second read attempt would throw. Parsing
  // from the captured text avoids that entirely.
  const rawBodyText = await res.text();
  let data = {};
  try {
    data = rawBodyText ? JSON.parse(rawBodyText) : {};
  } catch {
    data = {};
  }

  if (!res.ok) {
    await logDealerOSFailureDiagnostics(res, rawBodyText, endpoint, requestHeadersForLog, payload);
    throw new Error(`DealerOS lead creation failed: ${res.status} — ${rawBodyText || "{}"}`);
  }

  const externalLeadId = data?.data?.lead_id ?? null;
  console.log(`  ✅ [dealeros] Lead created for dealership ${dest.dealershipId} — DealerOS lead_id: ${externalLeadId}`);
  return { ...data, externalLeadId };
}
