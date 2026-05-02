"""
Filing content summarizer — downloads a filing and calls Claude to produce
a concise bullet-point summary that is embedded in email alerts.
"""

import asyncio
import base64
import html
import logging
import re
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_MODEL             = "claude-haiku-4-5"
_MAX_BYTES         = 4 * 1024 * 1024   # 4 MB download limit
_MAX_TEXT_CHARS    = 40_000             # truncate plain/HTML text before sending

_SYSTEM = (
    "You are a concise financial analyst. "
    "Always respond in English, regardless of the language of the filing. "
    "Summarize the key points of the provided company filing in exactly 3-5 bullet points. "
    "Each bullet must start with '• '. "
    "Cover: what the filing is about, any key figures or events, and material implications. "
    "Be factual and specific. Plain language only."
)


async def _download(url: str) -> tuple[bytes, str]:
    """Returns (raw_bytes, content_type)."""
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        res = await client.get(
            url,
            headers={"User-Agent": "GlobalFilings research@globalfilings.com"},
        )
        res.raise_for_status()
        return res.content, res.headers.get("content-type", "")


def _strip_html(text: str) -> str:
    text = re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<style[^>]*>.*?</style>",  " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&[a-z]+;", " ", text)
    return re.sub(r"\s+", " ", text).strip()


async def summarize_filing(url: str, title: str = "", api_key: str = "") -> Optional[str]:
    """
    Download *url* and return a Claude-generated bullet-point summary string,
    or None if no key is provided / download fails / Claude errors.
    """
    if not api_key or not url:
        return None

    try:
        raw, content_type = await asyncio.wait_for(_download(url), timeout=35)
    except Exception as e:
        logger.debug("summarizer: could not download %s — %s", url, e)
        return None

    raw = raw[:_MAX_BYTES]
    is_pdf = "pdf" in content_type.lower() or url.lower().split("?")[0].endswith(".pdf")

    try:
        from anthropic import AsyncAnthropic
        client = AsyncAnthropic(api_key=api_key)

        if is_pdf:
            b64 = base64.standard_b64encode(raw).decode()
            user_content = [
                {
                    "type": "document",
                    "source": {
                        "type": "base64",
                        "media_type": "application/pdf",
                        "data": b64,
                    },
                },
                {
                    "type": "text",
                    "text": (
                        f"Summarize this filing in 3-5 bullet points."
                        + (f" Filing title: {title}" if title else "")
                    ),
                },
            ]
        else:
            try:
                text = raw.decode("utf-8", errors="replace")
            except Exception:
                return None
            if "<html" in text.lower() or "<body" in text.lower():
                text = _strip_html(text)
            text = text[:_MAX_TEXT_CHARS]
            user_content = (
                f"Filing title: {title}\n\n{text}"
                "\n\nSummarize the above filing in 3-5 bullet points."
            )

        response = await client.messages.create(
            model=_MODEL,
            max_tokens=512,
            system=_SYSTEM,
            messages=[{"role": "user", "content": user_content}],
        )
        return response.content[0].text.strip()

    except Exception as e:
        logger.warning("summarizer: Claude call failed for %s — %s", url, e)
        return None


def format_summary_html(summary: str) -> str:
    """Convert a bullet-point summary string to safe inline HTML."""
    lines = [l.strip() for l in summary.splitlines() if l.strip()]
    items = ""
    for line in lines:
        # strip leading bullet characters from Claude's output
        clean = re.sub(r"^[•\-\*]\s*", "", line)
        items += (
            f'<li style="margin-bottom:4px;color:#9ca3af;font-size:12px;line-height:1.5">'
            f"{html.escape(clean)}</li>"
        )
    if not items:
        return ""
    return (
        '<div style="margin-top:8px">'
        '<div style="font-size:10px;font-weight:600;color:#4b5563;'
        'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px">AI Summary</div>'
        f'<ul style="margin:0;padding-left:16px">{items}</ul>'
        "</div>"
    )
