"""
Email alerts via Brevo SMTP.
"""

import logging
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

SMTP_HOST = os.environ.get("BREVO_SMTP_HOST", "smtp-relay.brevo.com")
SMTP_PORT = int(os.environ.get("BREVO_SMTP_PORT", 587))
SMTP_USER = os.environ.get("BREVO_SMTP_USER", "")
SMTP_PASS = os.environ.get("BREVO_SMTP_PASS", "")
FROM_EMAIL = os.environ.get("ALERT_FROM_EMAIL", SMTP_USER)


def _html_body(company_name: str, ticker: str | None, filings: list[dict]) -> str:
    ticker_str = f" ({ticker})" if ticker else ""
    rows = ""
    for f in filings:
        url   = f.get("url", "")
        title = f.get("title", "Untitled")
        cat   = f.get("category_name") or f.get("cat", "")
        date  = f.get("broadcast_date_time") or f.get("submission_date", "")
        link  = f"https://links.sgx.com/1.0.0/corporate-announcements/{url}" if url and not url.startswith("http") else url
        rows += f"""
        <tr>
          <td style="padding:12px 16px;border-bottom:1px solid #222;font-size:14px;color:#e5e7eb">{title}</td>
          <td style="padding:12px 16px;border-bottom:1px solid #222;font-size:12px;color:#9ca3af;white-space:nowrap">{cat}</td>
          <td style="padding:12px 16px;border-bottom:1px solid #222;font-size:12px;color:#9ca3af;white-space:nowrap">{date}</td>
          <td style="padding:12px 16px;border-bottom:1px solid #222;font-size:12px">
            {"<a href='" + link + "' style='color:#60a5fa;text-decoration:none'>View →</a>" if link else "—"}
          </td>
        </tr>"""

    count = len(filings)
    plural = "filing" if count == 1 else "filings"

    return f"""<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#0a0a0a;font-family:'Inter',Arial,sans-serif">
  <div style="max-width:600px;margin:40px auto;background:#111;border:1px solid #222;border-radius:12px;overflow:hidden">

    <div style="padding:24px 28px;border-bottom:1px solid #222;background:#0f0f0f">
      <div style="font-size:12px;color:#6b7280;margin-bottom:6px;text-transform:uppercase;letter-spacing:0.5px">SGX Filing Alert</div>
      <div style="font-size:20px;font-weight:600;color:#fff">{company_name}{ticker_str}</div>
      <div style="font-size:13px;color:#9ca3af;margin-top:4px">{count} new {plural} on Singapore Exchange</div>
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


def send_filing_alert(to_email: str, company_name: str, ticker: str | None, filings: list[dict]) -> bool:
    if not SMTP_USER or not SMTP_PASS:
        logger.warning("SMTP credentials not configured — skipping email to %s", to_email)
        return False

    count  = len(filings)
    plural = "filing" if count == 1 else "filings"
    subject = f"[SGX Alert] {company_name} — {count} new {plural}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"Global Filings Alerts <{FROM_EMAIL}>"
    msg["To"]      = to_email
    msg.attach(MIMEText(_html_body(company_name, ticker, filings), "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(FROM_EMAIL, to_email, msg.as_string())
        logger.info("Alert sent to %s for %s (%d filings)", to_email, company_name, count)
        return True
    except Exception as e:
        logger.error("Failed to send alert to %s: %s", to_email, e)
        return False
