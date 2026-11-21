"""Optional real browser capture: pip install playwright; use installed Edge/Chrome.

Run only after dbt has built reports. This never fabricates chart pixels or data.
"""
import argparse
import os

from playwright.sync_api import sync_playwright

from manage import ROOT


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--browser', required=True, help='Installed Chromium/Edge executable path')
    args = parser.parse_args()
    destination = ROOT / 'docs/screenshots/financial-operations.png'
    destination.parent.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(executable_path=args.browser, headless=True)
        # A tall viewport loads Grafana's lazy panels before the capture.
        page = browser.new_page(viewport={'width': 1600, 'height': 2700}, device_scale_factor=1)
        page.goto('http://localhost:3000/login', wait_until='networkidle')
        page.locator('input[name="user"]').fill('admin')
        page.locator('input[name="password"]').fill(os.getenv('GRAFANA_PASSWORD', 'local-dashboard'))
        page.get_by_role('button', name='Log in', exact=True).click()
        page.wait_for_url(lambda url: '/login' not in url, timeout=30000)
        page.goto('http://localhost:3000/d/biglobal-financial?orgId=1&kiosk', wait_until='networkidle')
        page.wait_for_timeout(10000)
        page.screenshot(path=str(destination), full_page=True)
        browser.close()
    print(destination)


if __name__ == '__main__':
    main()
