FROM apache/superset:master

USER root

# Set environment variable for Playwright
ENV CONFIG_PATH_ENV_VAR=/app/superset_config.py
ENV PLAYWRIGHT_BROWSERS_PATH=/usr/local/share/playwright-browsers

RUN pip install \
    # install psycopg2 for using PostgreSQL metadata store - could be a MySQL package if using that backend:
    clickhouse-connect \
    psycopg2-binary \
    # add the driver(s) for your data warehouse(s), in this example we're showing for Microsoft SQL Server:
    pymssql \
    # package needed for using single-sign on authentication:
    Authlib \
    # install Playwright for taking screenshots for Alerts & Reports. This assumes the feature flag PLAYWRIGHT_REPORTS_AND_THUMBNAILS is enabled
    # That feature flag will default to True starting in 6.0.0
    # Playwright works only with Chrome.
    # If you are still using Selenium instead of Playwright, you would instead install here the selenium package and a headless browser & webdriver
    playwright \
    && PLAYWRIGHT_BROWSERS_PATH=/usr/local/share/playwright-browsers playwright install chromium

# Switch back to the superset user
USER superset
