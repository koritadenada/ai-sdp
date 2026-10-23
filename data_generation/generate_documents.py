"""Generate realistic sample documents (PDF, DOCX, PNG) for the pipeline.

Produces one file per format from a shared corpus so the pipeline can
demonstrate ai_parse_document across document types. Output goes to
data_generation/generated_docs/ ; seed.sh uploads that folder to the raw volume.

Run:  .venv/bin/python data_generation/generate_documents.py
"""

from __future__ import annotations

import textwrap
from pathlib import Path

from docx import Document
from fpdf import FPDF
from PIL import Image, ImageDraw, ImageFont

OUT = Path(__file__).parent / "generated_docs"

# Shared corpus: (slug, format, title, body). Distinct topics so ai_classify has
# signal, and a mix of PDF / DOCX / PNG so ai_parse_document sees every type.
# CORPUS is the initial load (seed.sh uploads it); LATER_DOCS are dropped into
# the volume one at a time afterwards to show an incremental, one-file run.
CORPUS = [
    ("password_reset", "pdf", "Resetting Your Account Password",
        "If you have forgotten your password, click the Forgot Password link on the "
        "sign-in page. We will email you a secure reset link that stays valid for "
        "thirty minutes. For security reasons the link can only be used once. If it "
        "expires, simply request another one. Customers who still cannot sign in after "
        "resetting should contact our support team, who can verify identity and "
        "manually trigger a reset."),
    ("enterprise_pricing", "docx", "Enterprise Pricing and Volume Discounts",
        "Our Enterprise plan is billed annually and includes unlimited seats, priority "
        "support, and a dedicated success manager. Volume discounts begin at one "
        "hundred seats and increase in tiers up to one thousand seats. Annual "
        "prepayment earns an additional discount. Invoices are issued at the start of "
        "each billing period and are payable within thirty days. Customers may upgrade "
        "mid-term and the difference is prorated."),
    ("analytics_launch", "png", "Announcing the Real-Time Analytics Dashboard",
        "We are excited to announce the launch of our real-time analytics dashboard. "
        "The new dashboard streams events with sub-second latency and lets teams build "
        "custom visualizations without writing code. Early customers report that "
        "decision cycles have shortened dramatically. The feature is available today on "
        "all paid plans and requires no configuration to get started."),
    ("data_processing_agreement", "pdf", "Data Processing Agreement Summary",
        "This data processing agreement governs how personal data is handled on your "
        "behalf. We act as a processor and only process data according to your "
        "documented instructions. Sub-processors are listed publicly and customers are "
        "notified before any change. All data is encrypted in transit and at rest. You "
        "may request deletion of personal data at any time and we will comply within "
        "the timelines required by applicable law."),
    ("mobile_v3_release", "png", "Announcing Mobile App Version 3.0",
        "Version 3.0 of our mobile app is now available on iOS and Android. This "
        "release introduces offline mode, letting you view dashboards without a network "
        "connection, and a redesigned navigation bar that puts your most-used reports "
        "one tap away. Push notifications are now fully configurable per workspace. We "
        "also cut cold-start time roughly in half. Update from your app store today; "
        "existing sessions will migrate automatically with no action required."),
    ("onboarding_checklist", "docx", "New Customer Onboarding Checklist",
        "Welcome aboard. To get your workspace ready, start by inviting your team "
        "members and assigning roles. Next, connect your first data source using the "
        "guided setup wizard and verify that ingestion completes without errors. "
        "Configure single sign-on so everyone signs in with your corporate identity "
        "provider. Finally, build your first dashboard from a template and share it "
        "with stakeholders. Our onboarding specialists are available for a live "
        "walkthrough during your first two weeks."),
    ("refund_policy", "docx", "Refund and Cancellation Policy",
        "Customers on monthly plans may cancel at any time and will not be billed for "
        "the following cycle; access continues until the end of the paid period. Annual "
        "plans are eligible for a prorated refund within the first thirty days of the "
        "term. Refunds are issued to the original payment method and typically settle "
        "within five to ten business days. Usage-based charges already incurred are "
        "non-refundable. To request a cancellation or refund, contact billing support "
        "with your account identifier and the invoice number in question."),
    ("sla_terms", "pdf", "Service Level Agreement Terms",
        "This service level agreement defines the availability commitment for the "
        "platform. We target monthly uptime of at least ninety-nine point nine percent, "
        "measured excluding scheduled maintenance announced at least forty-eight hours "
        "in advance. If monthly availability falls below the committed threshold, "
        "affected customers are eligible for service credits calculated as a percentage "
        "of the monthly fee. Claims must be submitted within thirty days of the "
        "incident. This agreement does not cover outages caused by factors outside our "
        "reasonable control, including customer network failures."),
    ("two_factor_authentication", "pdf", "Enabling Two-Factor Authentication",
        "Two-factor authentication adds a second layer of security to your account. "
        "After entering your password, you confirm your identity with a one-time code "
        "from an authenticator app or an SMS message. To turn it on, open Account "
        "Settings, choose Security, and select Enable Two-Factor Authentication. We "
        "recommend an authenticator app over SMS because codes are generated on your "
        "device and cannot be intercepted. Save your backup codes somewhere safe: if "
        "you lose access to your device, a backup code is the only way to sign in "
        "without contacting support."),
    ("vpn_troubleshooting", "pdf", "Troubleshooting VPN Connection Failures",
        "If the corporate VPN fails to connect, first confirm your device has a stable "
        "internet connection and that the client software is updated to the latest "
        "release. Clear any saved profiles, then re-import the configuration file sent "
        "by IT. Connections that drop repeatedly are usually caused by an unstable "
        "wireless network or an expired certificate. If the client reports an "
        "authentication error, reset your password and try again. Persistent failures "
        "after these steps should be raised with the support desk, who can inspect the "
        "gateway logs and re-provision your access token."),
]

LATER_DOCS = [
    ("api_rate_limits", "pdf", "API Rate Limits and Quotas",
        "Every API key is subject to rate limits that protect the platform for all "
        "customers. Standard plans allow six hundred requests per minute per key, and "
        "Enterprise plans allow three thousand. When a client exceeds its limit, the "
        "API returns HTTP status 429 with a Retry-After header that tells the client "
        "how many seconds to wait. We recommend exponential backoff with jitter for "
        "retries. Daily quotas reset at midnight UTC, and current usage is visible on "
        "the API usage page in the admin console. Customers who need higher limits can "
        "request an increase through their success manager."),
    ("sso_setup_guide", "pdf", "Setting Up Single Sign-On (SSO)",
        "Enterprise customers can let employees sign in with their company identity "
        "provider using single sign-on. We support SAML 2.0 and OpenID Connect, and "
        "have tested setups for Okta, Microsoft Entra ID and Google Workspace. To get "
        "started, an administrator opens Settings, then Security, then Single Sign-On, "
        "and copies our service provider metadata into the identity provider. After "
        "uploading the identity provider certificate, run the built-in test login "
        "before enforcing SSO for everyone. Once SSO is enforced, password sign-in is "
        "disabled for all users except two break-glass administrator accounts. Just-in- "
        "time provisioning creates an account automatically the first time a new "
        "employee signs in, and SCIM keeps users and groups in sync and removes access "
        "when someone leaves. Certificates expire, so set a reminder to rotate the "
        "identity provider certificate at least thirty days before its expiry date to "
        "avoid a sign-in outage."),
    ("data_retention_policy", "pdf", "Data Retention and Deletion Policy",
        "This policy explains how long we keep customer data and how it is deleted. "
        "Account and workspace data is kept for as long as your subscription is active. "
        "Application logs are kept for ninety days and audit logs for one year, after "
        "which they are permanently deleted. Backups are encrypted and rotated every "
        "thirty-five days. When a subscription ends, customer data is kept for a "
        "thirty-day grace period so it can be exported, and is then deleted from "
        "production systems and, within a further thirty-five days, from all backups. "
        "Customers on the Enterprise plan can request a shorter or longer retention "
        "period in their contract. Individual users can ask for their personal data to "
        "be deleted at any time through the privacy portal, and we confirm completion "
        "by email within thirty days."),
]


def make_pdf(slug: str, title: str, body: str, out: Path = OUT) -> None:
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.multi_cell(0, 10, title)
    pdf.ln(4)
    pdf.set_font("Helvetica", size=12)
    pdf.multi_cell(0, 8, body)
    pdf.output(str(out / f"{slug}.pdf"))


def make_docx(slug: str, title: str, body: str, out: Path = OUT) -> None:
    doc = Document()
    doc.add_heading(title, level=1)
    doc.add_paragraph(body)
    doc.save(str(out / f"{slug}.docx"))


def make_png(slug: str, title: str, body: str, out: Path = OUT) -> None:
    img = Image.new("RGB", (1000, 600), "white")
    draw = ImageDraw.Draw(img)
    try:
        title_font = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial Bold.ttf", 28)
        body_font = ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial.ttf", 20)
    except OSError:
        title_font = body_font = ImageFont.load_default()
    draw.text((40, 30), title, fill="black", font=title_font)
    y = 90
    for line in textwrap.wrap(body, width=80):
        draw.text((40, y), line, fill="black", font=body_font)
        y += 30
    img.save(str(out / f"{slug}.png"))


def main() -> None:
    makers = {"pdf": make_pdf, "docx": make_docx, "png": make_png}
    for out_dir, docs in ((OUT, CORPUS), (OUT / "later", LATER_DOCS)):
        out_dir.mkdir(parents=True, exist_ok=True)
        for slug, fmt, title, body in docs:
            makers[fmt](slug, title, body, out_dir)
            print(f"  wrote {out_dir.name}/{slug}.{fmt}")
    print(f">> Generated {len(CORPUS)} documents in {OUT} and {len(LATER_DOCS)} in {OUT / 'later'}")


if __name__ == "__main__":
    main()
