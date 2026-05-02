"""
Email alerts via Brevo HTTP API.
Uses port 443 (HTTPS) — works on Railway where SMTP ports are blocked.
"""

import logging
import os

import httpx

logger = logging.getLogger(__name__)

BREVO_API_KEY  = os.environ.get("BREVO_API_KEY", "")
FROM_EMAIL     = os.environ.get("ALERT_FROM_EMAIL", "alerts@globalfilings.com")
FROM_NAME      = "Global Filings Alerts"
BREVO_SEND_URL = "https://api.brevo.com/v3/smtp/email"

EXCHANGE_LABELS = {
    "sgx":    "Singapore Exchange",
    "asx":    "Australian Securities Exchange",
    "edgar":  "US SEC (EDGAR)",
    "edinet": "Japan TSE (EDINET)",
    "fca":    "UK FCA",
    "europe": "European Markets",
}


def _html_body(company_name: str, ticker: str | None, filings: list[dict], exchange: str) -> str:
    ticker_str     = f" ({ticker})" if ticker else ""
    exchange_label = EXCHANGE_LABELS.get(exchange.lower(), exchange.upper())

    rows = ""
    for f in filings:
        url   = f.get("url", "")
        title = f.get("title", "Untitled")
        cat   = f.get("category_name") or f.get("cat", "")
        date  = f.get("broadcast_date_time") or f.get("submission_date", "")
        link_html = f"<a href='{url}' style='color:#60a5fa;text-decoration:none'>View →</a>" if url else "—"
        rows += f"""
        <tr>
          <td style="padding:12px 16px;border-bottom:1px solid #222;font-size:14px;color:#e5e7eb">{title}</td>
          <td style="padding:12px 16px;border-bottom:1px solid #222;font-size:12px;color:#9ca3af;white-space:nowrap">{cat}</td>
          <td style="padding:12px 16px;border-bottom:1px solid #222;font-size:12px;color:#9ca3af;white-space:nowrap">{date}</td>
          <td style="padding:12px 16px;border-bottom:1px solid #222;font-size:12px">{link_html}</td>
        </tr>"""

    count  = len(filings)
    plural = "filing" if count == 1 else "filings"

    return f"""<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#0a0a0a;font-family:'Inter',Arial,sans-serif">
  <div style="max-width:600px;margin:40px auto;background:#111;border:1px solid #222;border-radius:12px;overflow:hidden">
    <div style="padding:24px 28px;border-bottom:1px solid #222;background:#0f0f0f">
      <div style="font-size:12px;color:#6b7280;margin-bottom:6px;text-transform:uppercase;letter-spacing:0.5px">{exchange_label} Filing Alert</div>
      <div style="font-size:20px;font-weight:600;color:#fff">{company_name}{ticker_str}</div>
      <div style="font-size:13px;color:#9ca3af;margin-top:4px">{count} new {plural} on {exchange_label}</div>
    </div>
    <table style="width:100%;border-collapse:collapse">
      <thead>
        <tr style="background:#0f0f0f">
          <th style="padding:10px 16px;text-align:left;font-size:10px;font-weight:600;color:#4b5563;text-transform:uppercase;letter-spacing:0.5px">Title</th>
          <th style="padding:10px 16px;text-align:left;font-size:10px;font-weight:600;color:#4b5563;text-transform:uppercase;letter-spacing:0.5px">Category</th>
          <th style="padding:10px 16px;text-align:left;font-size:10px;font-weight:600;color:#4b5563;text-transform:uppercase;letter-spacing:0.5px">Date</th>
          <th style="padding:10px 16px;text-align:left;font-size:10px;font-weight:600;color:#4b5563;text-transform:uppercase;letter-spacing:0.5px">Link</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
    <div style="padding:20px 28px;border-top:1px solid #222;font-size:11px;color:#4b5563;line-height:1.6">
      You're receiving this because <strong style="color:#6b7280">{company_name}</strong> is in your Global Filings watchlist.<br>
      <a href="https://filings-production.up.railway.app/" style="color:#6b7280">Manage watchlist →</a>
    </div>
  </div>
</body>
</html>"""


async def send_filing_alert(
    to_email: str,
    company_name: str,
    ticker: str | None,
    filings: list[dict],
    exchange: str = "sgx",
) -> bool:
    if not BREVO_API_KEY:
        logger.warning("BREVO_API_KEY not set — skipping email to %s", to_email)
        return False

    count   = len(filings)
    plural  = "filing" if count == 1 else "filings"
    subject = f"[{exchange.upper()} Alert] {company_name} — {count} new {plural}"

    payload = {
        "sender":      {"name": FROM_NAME, "email": FROM_EMAIL},
        "to":          [{"email": to_email}],
        "subject":     subject,
        "htmlContent": _html_body(company_name, ticker, filings, exchange),
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.post(
                BREVO_SEND_URL,
                headers={"api-key": BREVO_API_KEY, "Content-Type": "application/json"},
                json=payload,
            )
        if res.status_code in (200, 201):
            logger.info("Alert sent → %s | %s (%d filings)", to_email, company_name, count)
            return True
        logger.error("Brevo API %d for %s: %s", res.status_code, to_email, res.text[:200])
        return False
    except Exception as e:
        logger.error("Failed to send alert to %s: %s", to_email, e)
        return False
