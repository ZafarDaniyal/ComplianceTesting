# Insurance CRM Tracker

A lightweight shared CRM for insurance sales teams.

## What it includes

- Shared login for 4 salespeople + owner
- Sales entry fields: customer name, phone number, address, date sold, exact premium amount
- Monthly sales log and leaderboard
- Owner-only hidden analytics sheet
  - total premium sold
  - total agent commissions
  - total agency commissions (what comes to the business)
- Owner CSV upload and monthly CSV export
- Competition mode toggle
  - ON: everyone can see all sales and leaderboard
  - OFF: each salesperson sees only their own data
- Policy Change Confirmations (DocuSign-style, fast)
  - Build request in under a minute (remove/add car, driver, coverage, etc.)
  - Send via SMS, email, or manual copy
  - Customer opens one link and confirms/declines with typed signature
  - Full audit trail with timestamps, signature name, IP, and user agent

## Default users

- Owner: `owner / owner123!`
- Salespeople: `sales1`, `sales2`, `sales3`, `sales4` with passcode `agent123!`

## Run locally

```bash
cd "/Users/daniyalzafar/Documents/New project"
python3 app.py
```

Open [http://localhost:8080](http://localhost:8080).

## Policy change confirmation workflow

1. Open the **Change Confirmations** tab.
2. Add customer details + requested policy changes.
3. Choose channel:
   - `SMS` (uses `SMS_PROVIDER`; default is Textbelt, customer replies `YES`/`NO` where provider supports replies)
   - `Email` (uses SMTP if configured)
   - `Manual` (copy generated message + link)
4. SMS flow: customer replies `YES` or `NO`; CRM updates automatically.
5. Email/manual flow: customer opens `/confirm/<token>` and confirms/declines with signature.
6. Status is tracked in the confirmations table (`pending`, `confirmed`, `declined`, `expired`).

### Optional delivery setup

SMS provider switch:

- `SMS_PROVIDER`:
  - `textbelt` (default)
  - `twilio`
- `PUBLIC_BASE_URL` (recommended for local testing with tunnels):
  - Example: `https://your-tunnel.trycloudflare.com`
  - Used by the app when generating webhook/callback URLs.

SMS via Textbelt (default):

- `TEXTBELT_KEY`:
  - `textbelt` for free outbound trial (provider limits apply)
  - paid Textbelt key for higher limits and reply webhooks
- Reply webhook URL used by app (paid key):  
  `POST https://<your-domain>/api/sms/inbound?provider=textbelt`  
  Optional secret: append `&secret=<value>`

SMS via Twilio:

- `TWILIO_ACCOUNT_SID`
- `TWILIO_AUTH_TOKEN`
- `TWILIO_FROM_NUMBER`
- Set Twilio inbound webhook to:
  - `POST https://<your-domain>/api/sms/inbound?provider=twilio`
  - Optional security: set env `SMS_INBOUND_SECRET` and append `&secret=<value>` to the webhook URL

Email via SMTP:

- `SMTP_HOST`
- `SMTP_PORT` (default `587`)
- `SMTP_USERNAME` (optional)
- `SMTP_PASSWORD` (optional)
- `SMTP_FROM`
- `SMTP_USE_TLS` (`1` default, set `0` to disable)

## Data storage

SQLite database file:

- `/Users/daniyalzafar/Documents/New project/data/crm.db`

## Quick demo data

- In Owner Sheet, download `Sample Data CSV` and upload it.
- This loads realistic policy sales for all 4 salespeople so leaderboard and commissions are populated.

## Notes

- Change default passcodes before production use.
- This is designed for internal team usage and can be deployed behind your own hosting/login controls.

## Deploy free on Render (public URL)

1. Put this folder in a GitHub repo.
2. Go to [Render Dashboard](https://dashboard.render.com/) and click **New +** -> **Blueprint**.
3. Connect the GitHub repo.
4. Render will detect `render.yaml` and create the web service.
5. Wait for deploy, then open the `https://...onrender.com` URL.

### Deploy behavior for demo/testing

- Great for letting your friend tinker and even break data.
- SQLite data can reset on redeploy or instance changes, so treat this as a demo/staging environment.
- Default credentials are still active on first deploy, so change passcodes before wider sharing.
