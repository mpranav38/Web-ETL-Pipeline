"""
utils/notifier.py — Send run-summary alerts via Email (SMTP) and/or Slack webhook.

Configure in config.py under NOTIFICATIONS. Both channels are optional and
independent — configure one, both, or neither.

Email example (Gmail):
    EMAIL_CONFIG = EmailConfig(
        smtp_host="smtp.gmail.com",
        smtp_port=587,
        username="you@gmail.com",
        password="your-app-password",   # use an App Password, not your account password
        from_addr="you@gmail.com",
        to_addrs=["team@company.com"],
    )

Slack example:
    SLACK_CONFIG = SlackConfig(
        webhook_url="https://hooks.slack.com/services/T.../B.../xxx",
        channel="#data-ops",            # optional override
    )
"""

import logging
import smtplib
import ssl
from dataclasses import dataclass, field
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

import requests

logger = logging.getLogger(__name__)


# ─── Config dataclasses (add to config.py) ───────────────────────────────────

@dataclass
class EmailConfig:
    smtp_host: str
    smtp_port: int = 587
    username: str = ""
    password: str = ""
    from_addr: str = ""
    to_addrs: list[str] = field(default_factory=list)
    use_tls: bool = True


@dataclass
class SlackConfig:
    webhook_url: str
    channel: str = ""                   # Optional — overrides the webhook's default channel
    username: str = "ETL Pipeline"
    icon_emoji: str = ":bar_chart:"


# ─── Notifier ────────────────────────────────────────────────────────────────

class Notifier:
    """
    Sends run summaries and error alerts to configured channels.

    Usage:
        notifier = Notifier(email_cfg=EMAIL_CONFIG, slack_cfg=SLACK_CONFIG)
        notifier.notify(run_result, errors=[])
    """

    def __init__(
        self,
        email_cfg: Optional[EmailConfig] = None,
        slack_cfg: Optional[SlackConfig] = None,
    ):
        self.email_cfg = email_cfg
        self.slack_cfg = slack_cfg

    def notify(self, run_result: dict, errors: list[str] | None = None) -> None:
        """
        Send notifications after a pipeline run.

        Parameters
        ----------
        run_result : dict
            The dict returned by `pipeline.run_pipeline()`.
        errors : list[str]
            Any error messages collected during the run.
        """
        errors = errors or []
        status = "FAILED" if errors else "SUCCESS"
        subject = f"[ETL Pipeline] {status} — {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        body_text = self._build_text_body(run_result, errors, status)
        body_html = self._build_html_body(run_result, errors, status)

        if self.email_cfg:
            self._send_email(subject, body_text, body_html)

        if self.slack_cfg:
            self._send_slack(run_result, errors, status)

    def alert_error(self, source_name: str, error: str) -> None:
        """Send an immediate alert for a single extraction failure (optional)."""
        msg = f"*ETL Extraction Error* — `{source_name}`\n```{error}```"
        if self.slack_cfg:
            self._post_slack_message(msg)
        if self.email_cfg:
            self._send_email(
                subject=f"[ETL Pipeline] Error in '{source_name}'",
                body_text=f"Extraction error in '{source_name}':\n\n{error}",
                body_html=f"<p><b>Source:</b> {source_name}</p><pre>{error}</pre>",
            )

    # ── Email ─────────────────────────────────────────────────────────────

    def _send_email(self, subject: str, body_text: str, body_html: str) -> None:
        cfg = self.email_cfg
        if not cfg or not cfg.to_addrs:
            logger.warning("[Notifier] Email config incomplete — skipping.")
            return

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = cfg.from_addr or cfg.username
        msg["To"] = ", ".join(cfg.to_addrs)
        msg.attach(MIMEText(body_text, "plain"))
        msg.attach(MIMEText(body_html, "html"))

        try:
            context = ssl.create_default_context()
            with smtplib.SMTP(cfg.smtp_host, cfg.smtp_port) as server:
                if cfg.use_tls:
                    server.starttls(context=context)
                if cfg.username and cfg.password:
                    server.login(cfg.username, cfg.password)
                server.sendmail(cfg.from_addr or cfg.username, cfg.to_addrs, msg.as_string())
            logger.info("[Notifier] Email sent → %s", cfg.to_addrs)
        except Exception as exc:  # noqa: BLE001
            logger.error("[Notifier] Email failed: %s", exc)

    # ── Slack ─────────────────────────────────────────────────────────────

    def _send_slack(self, run_result: dict, errors: list[str], status: str) -> None:
        cfg = self.slack_cfg
        color = "#36a64f" if status == "SUCCESS" else "#e01e5a"

        sources_text = "\n".join(
            f"• *{s}*" for s in run_result.get("sources", [])
        ) or "_none_"

        outputs = run_result.get("outputs", {})
        csv_files = outputs.get("csv_files", [])
        excel_files = outputs.get("excel_files", [])

        fields = [
            {"title": "Status",          "value": status,                                  "short": True},
            {"title": "Duration",        "value": f"{run_result.get('elapsed_seconds', 0):.1f}s", "short": True},
            {"title": "Rows extracted",  "value": str(run_result.get("rows_extracted", 0)), "short": True},
            {"title": "Rows after clean","value": str(run_result.get("rows_after_cleaning", 0)), "short": True},
            {"title": "Sources",         "value": sources_text,                            "short": False},
            {"title": "CSV files",       "value": "\n".join(csv_files) or "_none_",        "short": False},
            {"title": "Excel files",     "value": "\n".join(excel_files) or "_none_",      "short": False},
        ]

        if errors:
            fields.append({"title": "Errors", "value": "\n".join(errors[:5]), "short": False})

        payload: dict = {
            "username": cfg.username,
            "icon_emoji": cfg.icon_emoji,
            "attachments": [{
                "color": color,
                "title": f"ETL Pipeline Run — {status}",
                "fields": fields,
                "footer": "ETL Pipeline",
                "ts": int(datetime.now().timestamp()),
            }],
        }
        if cfg.channel:
            payload["channel"] = cfg.channel

        self._post_slack_message(payload=payload)

    def _post_slack_message(self, text: str = "", payload: dict | None = None) -> None:
        cfg = self.slack_cfg
        if not cfg or not cfg.webhook_url:
            return
        body = payload or {"text": text, "username": cfg.username, "icon_emoji": cfg.icon_emoji}
        try:
            resp = requests.post(cfg.webhook_url, json=body, timeout=10)
            resp.raise_for_status()
            logger.info("[Notifier] Slack notification sent.")
        except Exception as exc:  # noqa: BLE001
            logger.error("[Notifier] Slack notification failed: %s", exc)

    # ── Message bodies ────────────────────────────────────────────────────

    @staticmethod
    def _build_text_body(run_result: dict, errors: list[str], status: str) -> str:
        lines = [
            f"ETL Pipeline Run — {status}",
            f"Timestamp : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Duration  : {run_result.get('elapsed_seconds', 0):.1f}s",
            f"Extracted : {run_result.get('rows_extracted', 0)} rows",
            f"After clean: {run_result.get('rows_after_cleaning', 0)} rows",
            "",
            "Sources:",
            *[f"  - {s}" for s in run_result.get("sources", [])],
            "",
            "Output files:",
            *[f"  CSV  : {f}" for f in run_result.get("outputs", {}).get("csv_files", [])],
            *[f"  Excel: {f}" for f in run_result.get("outputs", {}).get("excel_files", [])],
        ]
        if errors:
            lines += ["", "Errors:", *[f"  ! {e}" for e in errors]]
        return "\n".join(lines)

    @staticmethod
    def _build_html_body(run_result: dict, errors: list[str], status: str) -> str:
        color = "#2ecc71" if status == "SUCCESS" else "#e74c3c"
        sources_html = "".join(f"<li>{s}</li>" for s in run_result.get("sources", []))
        csv_html = "".join(
            f"<li><code>{f}</code></li>"
            for f in run_result.get("outputs", {}).get("csv_files", [])
        )
        excel_html = "".join(
            f"<li><code>{f}</code></li>"
            for f in run_result.get("outputs", {}).get("excel_files", [])
        )
        error_html = (
            "<h3 style='color:#e74c3c'>Errors</h3><ul>"
            + "".join(f"<li><code>{e}</code></li>" for e in errors)
            + "</ul>"
        ) if errors else ""

        return f"""
        <html><body style="font-family:sans-serif;max-width:600px;margin:auto">
        <h2 style="color:{color}">ETL Pipeline — {status}</h2>
        <table cellpadding="6" style="border-collapse:collapse;width:100%">
          <tr><td><b>Timestamp</b></td><td>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td></tr>
          <tr><td><b>Duration</b></td><td>{run_result.get('elapsed_seconds', 0):.1f}s</td></tr>
          <tr><td><b>Rows extracted</b></td><td>{run_result.get('rows_extracted', 0)}</td></tr>
          <tr><td><b>Rows after cleaning</b></td><td>{run_result.get('rows_after_cleaning', 0)}</td></tr>
        </table>
        <h3>Sources</h3><ul>{sources_html}</ul>
        <h3>Output files</h3>
        <b>CSV</b><ul>{csv_html or '<li>none</li>'}</ul>
        <b>Excel</b><ul>{excel_html or '<li>none</li>'}</ul>
        {error_html}
        </body></html>
        """
