"""
Module 8 — Authentication & RBAC
Sub-modules: registration confirmation link delivery, Forgot Password
email delivery.

Uses plain smtplib (no extra dependency) so this works with any SMTP
relay — Gmail app password, Mailtrap, SendGrid SMTP, etc. If SMTP isn't
configured (e.g. local dev without credentials yet), emails are logged
to stdout instead of raising, so registration/login flows still work
end-to-end for a demo even before real SMTP creds are wired up.
"""

from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from app.config import settings

logger = logging.getLogger("pms.email")


def _send(to_email: str, subject: str, html_body: str) -> None:
    if not settings.SMTP_HOST or not settings.SMTP_USER or not settings.SMTP_PASSWORD:
        logger.warning(
            "SMTP not configured — logging email instead of sending.\n"
            "TO: %s\nSUBJECT: %s\nBODY:\n%s", to_email, subject, html_body,
        )
        return

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = settings.SMTP_FROM
    message["To"] = to_email
    message.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
        server.starttls()
        server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
        server.sendmail(settings.SMTP_FROM, [to_email], message.as_string())


def send_verification_email(to_email: str, full_name: str, token: str) -> None:
    link = f"{settings.FRONTEND_URL}/verify-email.html?token={token}"
    html_body = f"""
    <p>Hi {full_name},</p>
    <p>Welcome to the Predictive Maintenance System. Please confirm your account
    by clicking the link below (valid for {settings.EMAIL_VERIFICATION_TTL_HOURS} hours):</p>
    <p><a href="{link}">{link}</a></p>
    """
    _send(to_email, "Confirm your Predictive Maintenance System account", html_body)


def send_password_reset_email(to_email: str, full_name: str, token: str) -> None:
    link = f"{settings.FRONTEND_URL}/reset-password.html?token={token}"
    html_body = f"""
    <p>Hi {full_name},</p>
    <p>We received a request to reset your password. This link is valid for
    {settings.PASSWORD_RESET_TTL_MINUTES} minutes:</p>
    <p><a href="{link}">{link}</a></p>
    <p>If you didn't request this, you can safely ignore this email.</p>
    """
    _send(to_email, "Reset your Predictive Maintenance System password", html_body)
